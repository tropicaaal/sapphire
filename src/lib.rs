//! Hi

use markov_str::MarkovChain;
use twilight_model::id::{marker::ChannelMarker, Id};
use worker::*;

pub mod db;
pub mod discord;

use crate::{
    db::MessageDatabase,
    discord::{Bot, ChannelMessagesFilter},
};

// splits messages up in a slightly smarter way than just whitespace splitting,
// taking into account some common discord-isms like markdown and such
pub const TOKEN_REGEX: &str = r#"(?xm)
    <a?:[\p{Alphabetic}\p{N}_]+:\d+> # custom emoji
  | <@!?\d+> # user mention <@id> and <@!id>
  | <@&\d+> # role mention <@&id>
  | <\#\d+> # channel mention <#id>
  | :[\p{Alphabetic}\p{N}_]+: # emoji shortcode :name:
  | https?://[^\s<>]+ # links
  | \*\*[^*\n]+\*\* # **bold**
  | \*[^*\n]+\* # *italics*
  | __[^_\n]+__ # __underline__
  | _[^_\n]+_ # _italics_
  | ~~[^~\n]+~~ # ~~strikethrough~~
  | \|\|[^|\n]+\|\| # ||spoiler||
  | ^[ \t]*[-*+][ \t] # bullet marker
  | ^[ \t]*>[ \t]? # blockquote marker
  | @[\p{Alphabetic}\p{N}_]+ # plain-text @mention
  | \n # explicit newline
  | (\p{Alphabetic}|\d)(\p{Alphabetic}|\d|'|-)*[.,!?;:]* # word separation w/ punctuation
"#;

// hardcoded, idgaf
pub const GAMING_PUNISHMENT: Id<ChannelMarker> = Id::new(1107876706322223154);
pub const CORPUS_CHANNELS: &[Id<ChannelMarker>] = &[
    Id::new(1107876706322223154), // #gaming-punishment
    Id::new(1107875275297017898), // #general-punishment
    Id::new(1128178649703653386), // #quotes
    Id::new(1116157169172234290), // #rules
];

#[event(start)]
fn start() {
    // say the line logjak
    console_log::init_with_level(log::Level::Debug).unwrap();
}

#[event(fetch)]
async fn fetch(_req: Request, _env: Env, _ctx: Context) -> Result<Response> {
    Response::redirect(worker::Url::parse("https://en.wikipedia.org/wiki/Trollface").unwrap())
}

#[event(scheduled)]
async fn scheduled(event: ScheduledEvent, env: Env, _ctx: ScheduleContext) {
    let mut db = MessageDatabase::new(env.d1("sapphire_messages").unwrap());
    let mut bot = Bot::new(&env.var("DISCORD_TOKEN").unwrap().to_string());

    log::info!("\nrunning cron job: {}", event.cron());

    match event.cron().as_str() {
        "*/20 * * * *" => update_db(&mut bot, &mut db).await,
        "* */6 * * *" => do_markov(&mut bot, &mut db).await,
        // when running under wrangler dev, cron jobs are manually triggered and don't
        // provide any useful metadata so we can't just wildcard to an unreachable guh
        _ => {
            update_db(&mut bot, &mut db).await;
            do_markov(&mut bot, &mut db).await;
        }
    }
}

async fn update_db(bot: &mut Bot, db: &mut MessageDatabase) {
    for channel in CORPUS_CHANNELS {
        if let Err(e) = sync_channel(bot, db, *channel).await {
            log::error!("[bot] failed to sync channel {channel}: {e}");
        }

        worker::Delay::from(std::time::Duration::from_millis(300)).await;
    }

    match db.prune_old_messages().await {
        Ok(n) => log::info!("[db] pruned {n} old messages"),
        Err(e) => log::error!("[db] failed to prune old messages: {e}"),
    }
}

async fn sync_channel(
    bot: &mut Bot,
    db: &mut MessageDatabase,
    channel: Id<ChannelMarker>,
) -> worker::Result<()> {
    let after_key = format!("after:{channel}");
    let before_key = format!("before:{channel}");
    let backfill_done_key = format!("backfill_done:{channel}");

    // catch up with the latest messages in the channel by paging forward
    let existing_after = db.get_state(&after_key).await?;
    let is_first_run = existing_after.is_none();
    let mut current_after = existing_after.and_then(|s| s.parse::<u64>().ok());
    let mut first_batch_min_id: Option<u64> = None;

    loop {
        let filter = current_after.map(ChannelMessagesFilter::After);
        let messages = bot.channel_messages(channel, filter, 100).await?;
        if messages.is_empty() {
            break;
        }
        log::info!(
            "[bot] fetched {} new messages from channel {channel}",
            messages.len()
        );

        let n_saved = db.save_messages(&messages).await?;
        log::info!("[db] saved {n_saved} new messages for channel {channel}");

        if first_batch_min_id.is_none() {
            first_batch_min_id = messages.iter().map(|m| m.id.get()).min();
        }

        let got_full_page = messages.len() == 100;
        if let Some(max_id) = messages.iter().map(|m| m.id.get()).max() {
            current_after = Some(current_after.map_or(max_id, |cur| cur.max(max_id)));
            db.set_state(&after_key, &current_after.unwrap().to_string())
                .await?;
        }

        if !got_full_page {
            break;
        }
    }

    // set the backfill cursor on first run so we don't immediately re-fetch the same page
    if is_first_run {
        if let Some(min_id) = first_batch_min_id {
            db.set_state(&before_key, &min_id.to_string()).await?;
        }
    }

    // now we backfill until we hit week-old messages. this only needs to be
    // done once per-channel because  (assuming cron jobs keep running) pruning
    // will always keep a 7-day window. we use the shitty kv state store to
    // set a permanent flag per-channel to remember that we've done this across
    // runs. ok bye
    if db.get_state(&backfill_done_key).await?.is_none() {
        let before = db
            .get_state(&before_key)
            .await?
            .and_then(|s| s.parse::<u64>().ok());
        let filter = before.map(ChannelMessagesFilter::Before);
        let messages = bot.channel_messages(channel, filter, 100).await?;

        log::info!(
            "[bot] backfilling {} older messages from channel {channel}",
            messages.len()
        );
        let n_saved = db.save_messages(&messages).await?;
        log::info!("[db] saved {n_saved} backfilled messages for channel {channel}");

        let oldest = messages.iter().min_by_key(|m| m.id.get()).unwrap();
        db.set_state(&before_key, &oldest.id.get().to_string())
            .await?;

        let hit_cutoff = (oldest.timestamp.as_micros() as i64) < db::one_week_ago_micros();
        let hit_channel_start = messages.len() < 100;

        if hit_cutoff || hit_channel_start {
            db.set_state(&backfill_done_key, "1").await?;
        }
    }

    Ok(())
}

async fn do_markov(bot: &mut Bot, db: &mut MessageDatabase) {
    let n_pruned = db.prune_old_messages().await.unwrap();
    if n_pruned > 0 {
        log::info!("[db] Pruned {n_pruned} messages from D1");
    }

    let corpus = db.load_corpus(2048).await.unwrap();
    log::info!("[db] Loaded corpus of size {}", corpus.len());

    let mut chain =
        MarkovChain::with_capacity(2, 8_000_000, regex::Regex::new(TOKEN_REGEX).unwrap());
    for sentence in corpus {
        chain.add_text(&sentence);
    }

    if let Some(msg) = &chain.generate(
        rand::Rng::gen_range(&mut rand::thread_rng(), 2..25),
        &mut rand::thread_rng(),
    ) {
        log::info!("[bot] Sending markov: {msg}");
        bot.create_message(GAMING_PUNISHMENT, &msg).await.unwrap();
    }
}

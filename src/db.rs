use twilight_model::channel::{message::MessageType, Message};
use worker::*;

pub fn one_week_ago_micros() -> i64 {
    worker::Date::now().as_millis() as i64 * 1000 - (7 * 24 * 60 * 60 * 1_000_000)
}

pub struct MessageDatabase {
    db: D1Database,
}

impl MessageDatabase {
    pub const fn new(db: D1Database) -> Self {
        Self { db }
    }

    pub async fn get_state(&mut self, key: &str) -> Result<Option<String>> {
        let stmt = self
            .db
            .prepare("SELECT value FROM state WHERE key = ?1")
            .bind(&[key.into()])?;
        let result = stmt.first::<serde_json::Value>(Some("value")).await?;
        Ok(result.and_then(|v| v.as_str().map(String::from)))
    }

    pub async fn set_state(&mut self, key: &str, value: &str) -> Result<()> {
        self.db
            .prepare(
                "INSERT INTO state (key, value) VALUES (?1, ?2)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            )
            .bind(&[key.into(), value.into()])?
            .run()
            .await?;
        Ok(())
    }

    pub async fn prune_old_messages(&mut self) -> Result<usize> {
        let one_week_ago = one_week_ago_micros();

        let result = self
            .db
            .prepare("DELETE FROM messages WHERE created_at < ?1")
            .bind(&[(one_week_ago as f64).into()])?
            .run()
            .await?;
        let deleted = result
            .meta()
            .ok()
            .flatten()
            .and_then(|m| m.changes)
            .unwrap_or(0) as usize;

        Ok(deleted)
    }

    pub async fn save_messages(&self, messages: &[Message]) -> Result<usize> {
        let mut statements = Vec::with_capacity(messages.len());
        let one_week_ago = one_week_ago_micros();
        for msg in messages {
            if msg.content.is_empty()
                || msg.application_id.is_some()
                || !matches!(
                    msg.kind,
                    MessageType::Regular | MessageType::Reply | MessageType::ThreadStarterMessage
                )
                || (msg.timestamp.as_micros() as i64) < one_week_ago
            {
                continue;
            }

            // include embed descriptions so she picks up tweets
            let mut content = msg.content.clone();
            for embed in &msg.embeds {
                if let Some(desc) = &embed.description {
                    content.push_str(" ");
                    content.push_str(desc);
                }
            }

            let stmt = self.db
                .prepare("INSERT OR IGNORE INTO messages (discord_id, content, created_at) VALUES (?1, ?2, ?3)")
                .bind(&[
                    msg.id.to_string().into(),
                    content.into(),
                    (msg.timestamp.as_micros() as f64).into(),
                ])?;
            statements.push(stmt);
        }

        if statements.is_empty() {
            return Ok(0);
        }

        let results = self.db.batch(statements).await?;

        let inserted = results
            .iter()
            .map(|r| r.meta().ok().flatten().and_then(|m| m.changes).unwrap_or(0) as usize)
            .sum();

        Ok(inserted)
    }

    pub async fn load_corpus(&self, max_messages: usize) -> Result<Vec<String>> {
        let stmt = self
            .db
            .prepare("SELECT content FROM messages ORDER BY created_at DESC LIMIT ?1")
            .bind(&[(max_messages as f64).into()])?;

        let results = stmt.all().await?;
        let rows: Vec<serde_json::Value> = results.results()?;

        Ok(rows
            .into_iter()
            .filter_map(|r| r.get("content").and_then(|c| c.as_str().map(String::from)))
            .collect())
    }
}

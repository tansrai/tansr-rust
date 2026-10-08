use super::*;

impl Session {
    /// Ask Serve's context manager to compact. Rejected/failed outcomes remain
    /// in the response; the SDK does not repeat billable model work.
    pub async fn compact(&self, input: CompactOptions, options: WriteOptions) -> Result<Value> {
        if input
            .instructions
            .as_deref()
            .unwrap_or_default()
            .encode_utf16()
            .count()
            > 4096
        {
            return Err(invalid("compaction instructions exceed 4096 UTF-16 units"));
        }
        if let Some(CheckpointOption::Labeled { label: Some(label) }) = &input.checkpoint {
            checkpoint_label(label)?;
        }
        let response = self
            .write(
                "session.compact",
                None,
                Some(serde_json::to_value(input)?),
                options,
            )
            .await?;
        let raw = object_response(&response, 200)?;
        validate_compaction(&raw)?;
        Ok(raw)
    }

    pub async fn checkpoints(&self) -> Result<Vec<Checkpoint>> {
        let response = self
            .read("session.checkpoint.list", BTreeMap::new())
            .await?;
        let raw = object_response(&response, 200)?;
        let items = raw
            .get("checkpoints")
            .and_then(Value::as_array)
            .ok_or_else(|| contract("checkpoint list requires an array"))?;
        items
            .iter()
            .map(|v| self.read_checkpoint(v.clone()))
            .collect()
    }

    pub async fn checkpoint(&self, label: &str, options: WriteOptions) -> Result<Checkpoint> {
        checkpoint_label(label)?;
        let response = self
            .write(
                "session.checkpoint.create",
                None,
                Some(json!({"label": label})),
                options,
            )
            .await?;
        self.read_checkpoint(object_response(&response, 201)?)
    }

    /// Restoration and context reconstruction belong to Serve. `checkpoint`
    /// requests a snapshot of the pre-restore state; it is not an archive ACK.
    pub async fn restore(
        &self,
        checkpoint_id: &str,
        checkpoint: bool,
        options: WriteOptions,
    ) -> Result<Value> {
        nonempty(checkpoint_id, "checkpoint ID")?;
        let response = self
            .write(
                "session.checkpoint.restore",
                Some(("targetId", checkpoint_id)),
                Some(json!({"checkpoint": checkpoint})),
                options,
            )
            .await?;
        let raw = object_response(&response, 200)?;
        if raw["status"] != "restored" || raw["checkpointId"] != checkpoint_id {
            return Err(contract(
                "restore receipt does not match requested checkpoint",
            ));
        }
        safe_field(&raw, "fromMessages")?;
        safe_field(&raw, "toMessages")?;
        Ok(raw)
    }

    pub async fn delete_checkpoint(
        &self,
        checkpoint_id: &str,
        options: WriteOptions,
    ) -> Result<()> {
        nonempty(checkpoint_id, "checkpoint ID")?;
        let response = self
            .write(
                "session.checkpoint.delete",
                Some(("targetId", checkpoint_id)),
                None,
                options,
            )
            .await?;
        if response.status != 204 || !response.body.is_empty() {
            return Err(contract("checkpoint deletion must return an empty 204"));
        }
        Ok(())
    }

    /// Preserve the exported bytes unchanged. Never parse/re-encode a snapshot.
    pub async fn export_checkpoint(&self, checkpoint_id: &str) -> Result<Vec<u8>> {
        nonempty(checkpoint_id, "checkpoint ID")?;
        let mut params = self.params();
        params.insert("targetId".into(), checkpoint_id.into());
        let response = self
            .client
            .call(
                "session.checkpoint.export",
                CallOptions {
                    params,
                    max_response_bytes: Some(MEDIA_BYTES),
                    ..CallOptions::default()
                },
            )
            .await?;
        if response.status != 200
            || response.meta.content_type != "application/octet-stream"
            || response.body.is_empty()
        {
            return Err(contract(
                "checkpoint export must be nonempty octet-stream bytes",
            ));
        }
        Ok(response.body)
    }

    pub async fn import_checkpoint(
        &self,
        data: Vec<u8>,
        label: &str,
        options: WriteOptions,
    ) -> Result<Checkpoint> {
        if data.is_empty() || data.len() > MEDIA_BYTES {
            return Err(invalid(
                "checkpoint must contain 1 to 32 MiB of original export bytes",
            ));
        }
        checkpoint_label(label)?;
        let mut call = write_options(options);
        call.raw_body = Some(data);
        if !label.is_empty() {
            call.query.insert("label".into(), label.into());
        }
        let response = self
            .write_call("session.checkpoint.import", None, call)
            .await?;
        self.read_checkpoint(object_response(&response, 201)?)
    }

    /// This changes the Serve session's configured workspace according to its
    /// own policy. It is not permission to access either host's filesystem.
    pub async fn set_cwd(&self, cwd: &str, options: WriteOptions) -> Result<Value> {
        nonempty(cwd, "cwd")?;
        let response = self
            .write("session.cwd.set", None, Some(json!({"cwd": cwd})), options)
            .await?;
        object_response(&response, 200)
    }

    /// Obtain an audio draft without sending it as a message automatically.
    pub async fn transcribe(
        &self,
        input: TranscriptionRequest,
        options: WriteOptions,
    ) -> Result<Value> {
        nonempty(&input.audio, "audio")?;
        let response = self
            .write(
                "session.audio.transcribe",
                None,
                Some(serde_json::to_value(input)?),
                options,
            )
            .await?;
        object_response(&response, 200)
    }

    /// HTTP 200 does not prove an audio artifact exists: inspect the returned
    /// platform result, including errorCode/taskId, before playback.
    pub async fn speak(&self, input: SpeechRequest, options: WriteOptions) -> Result<Value> {
        nonempty(&input.input, "speech input")?;
        if input
            .format
            .as_deref()
            .is_some_and(|f| !matches!(f, "mp3" | "wav"))
            || input.speed.is_some_and(|s| !s.is_finite())
        {
            return Err(invalid("invalid speech format or speed"));
        }
        let response = self
            .write(
                "session.audio.speak",
                None,
                Some(serde_json::to_value(input)?),
                options,
            )
            .await?;
        object_response(&response, 200)
    }

    fn read_checkpoint(&self, raw: Value) -> Result<Checkpoint> {
        let checkpoint_id = required_str(&raw, "checkpointId")?.to_owned();
        let session_id = required_str(&raw, "sessionId")?.to_owned();
        let message_count = safe_field(&raw, "messageCount")?;
        if session_id != self.id() {
            return Err(contract("checkpoint belongs to another session"));
        }
        Ok(Checkpoint {
            checkpoint_id,
            session_id,
            message_count,
            raw,
        })
    }
}

fn checkpoint_label(label: &str) -> Result<()> {
    if label.encode_utf16().count() > 120 {
        Err(invalid("checkpoint label exceeds 120 UTF-16 units"))
    } else {
        Ok(())
    }
}

fn validate_compaction(raw: &Value) -> Result<()> {
    match raw.get("status").and_then(Value::as_str) {
        Some("compacted") => {
            required_str(raw, "compactionId")?;
            let range = raw
                .get("removedRange")
                .and_then(Value::as_array)
                .filter(|a| a.len() == 2)
                .ok_or_else(|| contract("invalid compacted range"))?;
            let first = range[0].as_u64().filter(|n| *n <= MAX_SAFE_INTEGER);
            let last = range[1].as_u64().filter(|n| *n <= MAX_SAFE_INTEGER);
            if !matches!((first, last), (Some(a), Some(b)) if a <= b) {
                return Err(contract("invalid compacted range"));
            }
        }
        Some("rejected") => {
            if !matches!(
                raw.get("reason").and_then(Value::as_str),
                Some("empty_history" | "not_configured" | "hook_blocked")
            ) {
                return Err(contract("invalid compaction rejection"));
            }
        }
        Some("failed") => {
            required_str(raw, "reason")?;
        }
        _ => return Err(contract("unrecognized compaction result")),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_limit_counts_utf16_not_unicode_scalars() {
        assert!(checkpoint_label(&"🙂".repeat(60)).is_ok());
        assert!(checkpoint_label(&"🙂".repeat(61)).is_err());
    }

    #[test]
    fn compaction_rejections_are_not_success_and_ranges_are_checked() {
        for raw in [
            json!({"status":"rejected","reason":"hook_blocked","checkpointId":"before"}),
            json!({"status":"failed","reason":"model_error"}),
            json!({"status":"compacted","compactionId":"c1","removedRange":[0,2]}),
        ] {
            assert!(validate_compaction(&raw).is_ok());
        }
        for raw in [
            json!({"status":"compacted","compactionId":"c1","removedRange":[3,2]}),
            json!({"status":"compacted","compactionId":"c1","removedRange":[0,9_007_199_254_740_992u64]}),
            json!({"status":"failed"}),
            json!({"status":"accepted"}),
        ] {
            assert!(validate_compaction(&raw).is_err());
        }
    }
}

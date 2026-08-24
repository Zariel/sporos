use sporos_model::{ArrKind, ReleaseDescriptor};

const MAX_ALTERNATE_TITLES: usize = 32;

pub(crate) async fn replace(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    source_id: &[u8; 16],
    source_type: &str,
    release: &ReleaseDescriptor,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM sporos_source_title WHERE source_id = ? AND source_type = ?")
        .bind(source_id.as_slice())
        .bind(source_type)
        .execute(&mut **transaction)
        .await?;
    sqlx::query("DELETE FROM sporos_source_external_id WHERE source_id = ? AND source_type = ?")
        .bind(source_id.as_slice())
        .bind(source_type)
        .execute(&mut **transaction)
        .await?;
    insert_title(
        transaction,
        source_id,
        source_type,
        release.primary_title.as_str(),
        "primary",
    )
    .await?;
    for title in release.alternate_titles.iter().take(MAX_ALTERNATE_TITLES) {
        insert_title(
            transaction,
            source_id,
            source_type,
            title.as_str(),
            "alternate",
        )
        .await?;
    }
    if let Some(identity) = &release.arr_identity {
        let kind = match identity.kind {
            ArrKind::Movie => "movie",
            ArrKind::Series => "series",
        };
        insert_external(
            transaction,
            source_id,
            source_type,
            &format!("arr:{kind}:{}", identity.instance),
            &identity.entity_id.to_string(),
        )
        .await?;
        for (namespace, value) in [
            ("tvdb", identity.tvdb_id.map(|value| value.to_string())),
            ("tmdb", identity.tmdb_id.map(|value| value.to_string())),
            ("imdb", identity.imdb_id.clone()),
        ] {
            if let Some(value) = value {
                insert_external(transaction, source_id, source_type, namespace, &value).await?;
            }
        }
    }
    Ok(())
}

async fn insert_title(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    source_id: &[u8; 16],
    source_type: &str,
    title: &str,
    kind: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO sporos_source_title (source_id, source_type, normalized_title, kind)
         VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
    )
    .bind(source_id.as_slice())
    .bind(source_type)
    .bind(title)
    .bind(kind)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_external(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    source_id: &[u8; 16],
    source_type: &str,
    namespace: &str,
    value: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO sporos_source_external_id (source_id, source_type, namespace, value)
         VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
    )
    .bind(source_id.as_slice())
    .bind(source_type)
    .bind(namespace)
    .bind(value)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

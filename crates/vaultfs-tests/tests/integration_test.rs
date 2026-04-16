use std::path::PathBuf;

/// Test the storage engine directly
#[test]
fn test_storage_bucket_crud() {
    let dir = tempdir();
    let engine = vaultfs_storage::StorageEngine::new(dir.clone(), 1024 * 1024, true).unwrap();

    // Create bucket
    let bucket = engine
        .create_bucket(&vaultfs_storage::CreateBucketRequest {
            name: "test".to_string(),
            public: false,
        })
        .unwrap();
    assert_eq!(bucket.name, "test");

    // List buckets
    let buckets = engine.list_buckets().unwrap();
    assert_eq!(buckets.len(), 1);

    // Get bucket
    let b = engine.get_bucket("test").unwrap();
    assert_eq!(b.name, "test");

    // Duplicate bucket fails
    let err = engine
        .create_bucket(&vaultfs_storage::CreateBucketRequest {
            name: "test".to_string(),
            public: false,
        });
    assert!(err.is_err());

    // Delete bucket
    engine.delete_bucket("test").unwrap();
    let buckets = engine.list_buckets().unwrap();
    assert!(buckets.is_empty());
}

#[tokio::test]
async fn test_storage_object_crud() {
    let dir = tempdir();
    let engine = vaultfs_storage::StorageEngine::new(dir.clone(), 1024 * 1024, true).unwrap();

    engine
        .create_bucket(&vaultfs_storage::CreateBucketRequest {
            name: "files".to_string(),
            public: false,
        })
        .unwrap();

    // Put object
    let data = bytes::Bytes::from("hello world");
    let meta = engine
        .put_object("files", "test.txt", data.clone(), Some("text/plain"), None)
        .await
        .unwrap();
    assert_eq!(meta.key, "test.txt");
    assert_eq!(meta.size, 11);
    assert_eq!(meta.content_type, "text/plain");

    // Get object
    let (got_meta, got_data) = engine.get_object("files", "test.txt").await.unwrap();
    assert_eq!(got_data, data);
    assert_eq!(got_meta.sha256, meta.sha256);

    // Head object
    let head = engine.get_object_meta("files", "test.txt").unwrap();
    assert_eq!(head.size, 11);

    // List objects
    let list = engine
        .list_objects(&vaultfs_storage::ListObjectsRequest {
            bucket: "files".to_string(),
            prefix: None,
            delimiter: None,
            max_keys: None,
            continuation_token: None,
        })
        .unwrap();
    assert_eq!(list.objects.len(), 1);

    // Delete object
    engine.delete_object("files", "test.txt").await.unwrap();
    let result = engine.get_object("files", "test.txt").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_deduplication() {
    let dir = tempdir();
    let engine = vaultfs_storage::StorageEngine::new(dir.clone(), 1024 * 1024, true).unwrap();

    engine
        .create_bucket(&vaultfs_storage::CreateBucketRequest {
            name: "dedup".to_string(),
            public: false,
        })
        .unwrap();

    let data = bytes::Bytes::from("identical content");

    let meta1 = engine
        .put_object("dedup", "file1.txt", data.clone(), None, None)
        .await
        .unwrap();
    let meta2 = engine
        .put_object("dedup", "file2.txt", data.clone(), None, None)
        .await
        .unwrap();

    // Same hash
    assert_eq!(meta1.sha256, meta2.sha256);

    // Both readable
    let (_, d1) = engine.get_object("dedup", "file1.txt").await.unwrap();
    let (_, d2) = engine.get_object("dedup", "file2.txt").await.unwrap();
    assert_eq!(d1, d2);
}

#[tokio::test]
async fn test_object_too_large() {
    let dir = tempdir();
    let engine = vaultfs_storage::StorageEngine::new(dir.clone(), 100, true).unwrap();

    engine
        .create_bucket(&vaultfs_storage::CreateBucketRequest {
            name: "small".to_string(),
            public: false,
        })
        .unwrap();

    let data = bytes::Bytes::from(vec![0u8; 200]);
    let result = engine
        .put_object("small", "big.bin", data, None, None)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_virtual_directories() {
    let dir = tempdir();
    let engine = vaultfs_storage::StorageEngine::new(dir.clone(), 1024 * 1024, true).unwrap();

    engine
        .create_bucket(&vaultfs_storage::CreateBucketRequest {
            name: "dirs".to_string(),
            public: false,
        })
        .unwrap();

    engine
        .put_object("dirs", "docs/readme.md", bytes::Bytes::from("# README"), None, None)
        .await
        .unwrap();
    engine
        .put_object("dirs", "docs/guide.md", bytes::Bytes::from("# Guide"), None, None)
        .await
        .unwrap();
    engine
        .put_object("dirs", "images/logo.png", bytes::Bytes::from("PNG"), None, None)
        .await
        .unwrap();
    engine
        .put_object("dirs", "root.txt", bytes::Bytes::from("root"), None, None)
        .await
        .unwrap();

    // List with delimiter
    let list = engine
        .list_objects(&vaultfs_storage::ListObjectsRequest {
            bucket: "dirs".to_string(),
            prefix: None,
            delimiter: Some("/".to_string()),
            max_keys: None,
            continuation_token: None,
        })
        .unwrap();

    assert_eq!(list.objects.len(), 1); // root.txt
    assert_eq!(list.objects[0].key, "root.txt");
    assert!(list.common_prefixes.contains(&"docs/".to_string()));
    assert!(list.common_prefixes.contains(&"images/".to_string()));

    // List with prefix
    let list = engine
        .list_objects(&vaultfs_storage::ListObjectsRequest {
            bucket: "dirs".to_string(),
            prefix: Some("docs/".to_string()),
            delimiter: None,
            max_keys: None,
            continuation_token: None,
        })
        .unwrap();
    assert_eq!(list.objects.len(), 2);
}

#[test]
fn test_auth_keys() {
    let dir = tempdir();
    let auth = vaultfs_auth::AuthManager::open(&dir.join("auth.db")).unwrap();

    // Create key
    let (key, raw) = auth
        .create_key(
            "test",
            vaultfs_auth::Permissions {
                read: true,
                write: false,
                delete: false,
                admin: false,
            },
            vaultfs_auth::BucketAccess::All,
        )
        .unwrap();
    assert_eq!(key.name, "test");
    assert!(raw.starts_with("vfs_"));

    // Verify key
    let verified = auth.verify_key(&raw).unwrap();
    assert_eq!(verified.name, "test");
    assert!(verified.permissions.read);
    assert!(!verified.permissions.write);

    // Invalid key
    let result = auth.verify_key("vfs_invalid");
    assert!(result.is_err());

    // List keys
    let keys = auth.list_keys().unwrap();
    assert_eq!(keys.len(), 1);

    // Delete key
    auth.delete_key(&key.id).unwrap();
    let keys = auth.list_keys().unwrap();
    assert!(keys.is_empty());
}

#[test]
fn test_bucket_access_control() {
    let dir = tempdir();
    let auth = vaultfs_auth::AuthManager::open(&dir.join("auth.db")).unwrap();

    let (key, _) = auth
        .create_key(
            "limited",
            vaultfs_auth::Permissions::default(),
            vaultfs_auth::BucketAccess::Specific {
                buckets: vec!["allowed".to_string()],
            },
        )
        .unwrap();

    assert!(auth.check_bucket_access(&key, "allowed").is_ok());
    assert!(auth.check_bucket_access(&key, "forbidden").is_err());
}

#[test]
fn test_presigned_urls() {
    let gen = vaultfs_auth::PresignedUrlGenerator::new(b"test-secret-key");

    let req = vaultfs_auth::PresignRequest {
        method: "GET".to_string(),
        bucket: "mybucket".to_string(),
        key: "myfile.txt".to_string(),
        expires_in: Some(3600),
        content_type: None,
    };

    let presigned = gen.generate("http://localhost:8000", &req);
    assert!(presigned.url.contains("signature="));
    assert!(presigned.url.contains("expires="));
    assert_eq!(presigned.method, "GET");

    // Extract params and verify
    let url = &presigned.url;
    let query = url.split('?').nth(1).unwrap();
    let mut expires = 0i64;
    let mut signature = String::new();
    for param in query.split('&') {
        let (k, v) = param.split_once('=').unwrap();
        match k {
            "expires" => expires = v.parse().unwrap(),
            "signature" => signature = v.to_string(),
            _ => {}
        }
    }

    assert!(gen.verify("GET", "mybucket", "myfile.txt", expires, &signature));
    assert!(!gen.verify("PUT", "mybucket", "myfile.txt", expires, &signature));
    assert!(!gen.verify("GET", "mybucket", "myfile.txt", expires, "invalidsig"));
}

#[test]
fn test_image_transform_params() {
    use vaultfs_processing::*;

    let params = TransformParams {
        width: Some(300),
        height: Some(200),
        format: Some(OutputFormat::WebP),
        quality: Some(80),
        fit: FitMode::Cover,
    };
    assert!(params.has_transforms());
    let key = params.cache_key();
    assert!(key.contains("300"));
    assert!(key.contains("200"));

    let empty = TransformParams::default();
    assert!(!empty.has_transforms());
}

#[test]
fn test_format_detection() {
    use vaultfs_processing::*;

    assert!(matches!(
        best_format_from_accept("image/avif,image/webp,*/*"),
        Some(OutputFormat::Avif)
    ));
    assert!(matches!(
        best_format_from_accept("image/webp,*/*"),
        Some(OutputFormat::WebP)
    ));
    assert!(best_format_from_accept("text/html").is_none());
}

#[tokio::test]
async fn test_cache() {
    let cache = vaultfs_cache::Cache::new(1024 * 1024, None, 0);

    // Miss
    assert!(cache.get("key1").await.is_none());

    // Put + hit
    cache
        .put("key1", bytes::Bytes::from("data"), "text/plain")
        .await
        .unwrap();
    let (data, ct) = cache.get("key1").await.unwrap();
    assert_eq!(data, bytes::Bytes::from("data"));
    assert_eq!(ct, "text/plain");

    // Invalidate
    cache.invalidate("key1").await.unwrap();
    assert!(cache.get("key1").await.is_none());
}

#[tokio::test]
async fn test_garbage_collection() {
    let dir = tempdir();
    let engine = vaultfs_storage::StorageEngine::new(dir.clone(), 1024 * 1024, false).unwrap();

    engine
        .create_bucket(&vaultfs_storage::CreateBucketRequest {
            name: "gc".to_string(),
            public: false,
        })
        .unwrap();

    // Create and delete an object (without dedup, blob stays)
    engine
        .put_object("gc", "temp.txt", bytes::Bytes::from("temporary"), None, None)
        .await
        .unwrap();
    engine.delete_object("gc", "temp.txt").await.unwrap();

    // Create an orphan blob manually
    let orphan_path = dir.join("blobs/ff/ff");
    std::fs::create_dir_all(&orphan_path).unwrap();
    std::fs::write(orphan_path.join("ffff_orphan"), "orphan data").unwrap();

    let gc = vaultfs_storage::GarbageCollector::new(dir);
    let result = gc.collect(engine.db()).unwrap();
    assert!(result.orphans_removed > 0);
    assert!(result.bytes_freed > 0);
}

#[tokio::test]
async fn test_backup_and_restore() {
    let dir = tempdir();
    let engine = vaultfs_storage::StorageEngine::new(dir.clone(), 1024 * 1024, true).unwrap();

    engine
        .create_bucket(&vaultfs_storage::CreateBucketRequest {
            name: "backup-test".to_string(),
            public: false,
        })
        .unwrap();

    engine
        .put_object("backup-test", "file1.txt", bytes::Bytes::from("data1"), None, None)
        .await
        .unwrap();
    engine
        .put_object("backup-test", "file2.txt", bytes::Bytes::from("data2"), None, None)
        .await
        .unwrap();

    // Create backup
    let backup_dir = tempdir();
    let bm = vaultfs_storage::BackupManager::new(dir.clone());
    let result = bm.create_snapshot(engine.db(), &backup_dir).unwrap();
    assert!(result.db_size > 0);
    assert_eq!(result.blobs_copied, 2);

    // Verify backup files exist
    assert!(backup_dir.join("vaultfs.db").exists());
    assert!(backup_dir.join("blobs").exists());

    // Restore to a new location
    let restore_dir = tempdir();
    let bm2 = vaultfs_storage::BackupManager::new(restore_dir.clone());
    let restore_result = bm2.restore_snapshot(&backup_dir).unwrap();
    assert!(restore_result.db_restored);
    assert_eq!(restore_result.blobs_restored, 2);

    // Verify restored data is usable
    let engine2 = vaultfs_storage::StorageEngine::new(restore_dir, 1024 * 1024, true).unwrap();
    let (_, data) = engine2.get_object("backup-test", "file1.txt").await.unwrap();
    assert_eq!(data, bytes::Bytes::from("data1"));
}

#[tokio::test]
async fn test_bucket_export() {
    let dir = tempdir();
    let engine = vaultfs_storage::StorageEngine::new(dir.clone(), 1024 * 1024, true).unwrap();

    engine
        .create_bucket(&vaultfs_storage::CreateBucketRequest {
            name: "export-test".to_string(),
            public: false,
        })
        .unwrap();

    engine
        .put_object("export-test", "a.txt", bytes::Bytes::from("aaa"), None, None)
        .await
        .unwrap();
    engine
        .put_object("export-test", "b.txt", bytes::Bytes::from("bbb"), None, None)
        .await
        .unwrap();

    let export_dir = tempdir();
    let bm = vaultfs_storage::BackupManager::new(dir);
    let count = bm.export_bucket(engine.db(), "export-test", &export_dir).unwrap();
    assert_eq!(count, 2);
    assert!(export_dir.join("a.txt").exists());
    assert!(export_dir.join("b.txt").exists());
    assert!(export_dir.join("_manifest.json").exists());
}

#[tokio::test]
async fn test_streaming_upload() {
    let dir = tempdir();
    let engine = vaultfs_storage::StorageEngine::new(dir.clone(), 1024 * 1024 * 100, true).unwrap();

    engine
        .create_bucket(&vaultfs_storage::CreateBucketRequest {
            name: "stream".to_string(),
            public: false,
        })
        .unwrap();

    // Simulate a stream of chunks
    let chunks: Vec<Result<bytes::Bytes, std::io::Error>> = vec![
        Ok(bytes::Bytes::from("chunk1")),
        Ok(bytes::Bytes::from("chunk2")),
        Ok(bytes::Bytes::from("chunk3")),
    ];
    let stream = futures::stream::iter(chunks);

    let meta = engine
        .put_object_stream("stream", "streamed.txt", stream, Some("text/plain"), None)
        .await
        .unwrap();
    assert_eq!(meta.size, 18); // chunk1 + chunk2 + chunk3

    // Verify content
    let (_, data) = engine.get_object("stream", "streamed.txt").await.unwrap();
    assert_eq!(data, bytes::Bytes::from("chunk1chunk2chunk3"));
}

fn tempdir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("vaultfs-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

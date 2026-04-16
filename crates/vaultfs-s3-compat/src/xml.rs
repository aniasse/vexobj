use vaultfs_storage::{Bucket, ListObjectsResponse, ObjectMeta};

pub fn error_xml(code: &str, message: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{code}</Code>
  <Message>{message}</Message>
</Error>"#
    )
}

pub fn list_buckets_xml(buckets: &[Bucket], owner: &str) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <ID>vaultfs</ID>
    <DisplayName>"#);
    xml.push_str(owner);
    xml.push_str(r#"</DisplayName>
  </Owner>
  <Buckets>"#);

    for b in buckets {
        xml.push_str(&format!(
            r#"
    <Bucket>
      <Name>{}</Name>
      <CreationDate>{}</CreationDate>
    </Bucket>"#,
            b.name,
            b.created_at.to_rfc3339()
        ));
    }

    xml.push_str(
        r#"
  </Buckets>
</ListAllMyBucketsResult>"#,
    );
    xml
}

pub fn list_objects_v2_xml(
    bucket: &str,
    prefix: &str,
    resp: &ListObjectsResponse,
    max_keys: u32,
    delimiter: &str,
) -> String {
    let mut xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>{bucket}</Name>
  <Prefix>{prefix}</Prefix>
  <MaxKeys>{max_keys}</MaxKeys>
  <IsTruncated>{}</IsTruncated>
  <KeyCount>{}</KeyCount>"#,
        resp.is_truncated,
        resp.objects.len()
    );

    if !delimiter.is_empty() {
        xml.push_str(&format!("\n  <Delimiter>{delimiter}</Delimiter>"));
    }

    if let Some(ref token) = resp.next_continuation_token {
        xml.push_str(&format!(
            "\n  <NextContinuationToken>{token}</NextContinuationToken>"
        ));
    }

    for obj in &resp.objects {
        xml.push_str(&format!(
            r#"
  <Contents>
    <Key>{}</Key>
    <LastModified>{}</LastModified>
    <ETag>"{}"</ETag>
    <Size>{}</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>"#,
            xml_escape(&obj.key),
            obj.updated_at.to_rfc3339(),
            obj.sha256,
            obj.size,
        ));
    }

    for prefix in &resp.common_prefixes {
        xml.push_str(&format!(
            r#"
  <CommonPrefixes>
    <Prefix>{}</Prefix>
  </CommonPrefixes>"#,
            xml_escape(prefix),
        ));
    }

    xml.push_str("\n</ListBucketResult>");
    xml
}

pub fn copy_object_xml(meta: &ObjectMeta) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult>
  <LastModified>{}</LastModified>
  <ETag>"{}"</ETag>
</CopyObjectResult>"#,
        meta.updated_at.to_rfc3339(),
        meta.sha256,
    )
}

pub fn create_bucket_xml() -> String {
    String::new()
}

pub fn delete_result_xml(key: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Deleted>
    <Key>{}</Key>
  </Deleted>
</DeleteResult>"#,
        xml_escape(key),
    )
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

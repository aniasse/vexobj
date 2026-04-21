use vexobj_storage::{Bucket, ListObjectsResponse, MultipartPart, ObjectMeta};

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
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <ID>vexobj</ID>
    <DisplayName>"#,
    );
    xml.push_str(owner);
    xml.push_str(
        r#"</DisplayName>
  </Owner>
  <Buckets>"#,
    );

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

pub fn initiate_multipart_xml(bucket: &str, key: &str, upload_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>{}</Bucket>
  <Key>{}</Key>
  <UploadId>{}</UploadId>
</InitiateMultipartUploadResult>"#,
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id),
    )
}

pub fn complete_multipart_xml(bucket: &str, key: &str, etag: &str, location: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>{}</Location>
  <Bucket>{}</Bucket>
  <Key>{}</Key>
  <ETag>"{}"</ETag>
</CompleteMultipartUploadResult>"#,
        xml_escape(location),
        xml_escape(bucket),
        xml_escape(key),
        etag,
    )
}

pub fn list_parts_xml(
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: &[MultipartPart],
    max_parts: u32,
) -> String {
    let mut xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>{}</Bucket>
  <Key>{}</Key>
  <UploadId>{}</UploadId>
  <MaxParts>{}</MaxParts>
  <IsTruncated>false</IsTruncated>"#,
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id),
        max_parts,
    );
    for p in parts {
        xml.push_str(&format!(
            r#"
  <Part>
    <PartNumber>{}</PartNumber>
    <LastModified>{}</LastModified>
    <ETag>"{}"</ETag>
    <Size>{}</Size>
  </Part>"#,
            p.part_number,
            p.uploaded_at.to_rfc3339(),
            p.etag,
            p.size,
        ));
    }
    xml.push_str("\n</ListPartsResult>");
    xml
}

/// Parse a CompleteMultipartUpload request body. S3 sends XML like:
/// `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"x"</ETag></Part>...`
/// Returns `Vec<(part_number, etag_unquoted)>` in the order the client gave us.
pub fn parse_complete_multipart(body: &str) -> Option<Vec<(u32, String)>> {
    // Tiny hand-rolled parser. We could pull in quick-xml but the body is
    // always small and shallow, so a two-pass regex-free scan is enough
    // and avoids growing the dep tree.
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(idx) = rest.find("<Part>") {
        rest = &rest[idx + "<Part>".len()..];
        let end = rest.find("</Part>")?;
        let block = &rest[..end];
        let pn = extract_tag(block, "PartNumber")?.parse::<u32>().ok()?;
        let etag = extract_tag(block, "ETag")?
            .trim_matches('"')
            .trim()
            .to_string();
        out.push((pn, etag));
        rest = &rest[end + "</Part>".len()..];
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn extract_tag<'a>(block: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = block.find(&open)? + open.len();
    let end = block[start..].find(&close)? + start;
    Some(block[start..end].trim())
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

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

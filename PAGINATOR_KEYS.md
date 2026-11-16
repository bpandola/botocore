# Paginator Configuration Keys

An analysis of every `paginators*.json` file under `botocore/data/`: 515 files
in total, made up of 462 `paginators-1.json` and 53 `paginators-1.sdk-extras.json`.
The meaning of each key was checked against how `botocore/paginate.py` and
`botocore/loaders.py` use it.

## File-level keys

| Key | Where | Meaning |
|---|---|---|
| `pagination` | `paginators-1.json` | Maps each operation name (e.g. `ListObjects`) to its pagination config. |
| `version` | `sdk-extras` files | Schema version of the extras file (`1.0` or `1`). |
| `merge` | `sdk-extras` files | An object deep-merged into the main paginator model when it loads (`loaders.py`, `deep_merge(model, extra_model['merge'])`). In practice it is only used to add `non_aggregate_keys` to existing operations. |

## Per-operation keys

There are 3,541 paginated operations. Only six keys ever appear inside an
operation's config:

| Key | Count | Type | Required? | Meaning |
|---|---|---|---|---|
| `input_token` | 3541 | str, or list (3 cases) | Yes | The name of the **request** parameter that carries the continuation token, e.g. `NextToken`, `Marker`, `StartRecordName`. It is a plain parameter name, not an expression. |
| `output_token` | 3541 | str, or list (3 cases) | Yes | A **JMESPath expression** on the response that gives the next token. If it comes back empty or falsy, pagination stops. When it is a list, it pairs up position by position with `input_token`. |
| `result_key` | 3541 | str, or list (32 cases) | Yes, in practice | JMESPath expression(s) for the actual results in each page. `build_full_result()` combines these across pages, `result_key_iters()` iterates them, and `MaxItems` truncation counts them. If there is a list, the first entry is the "primary" key used for `MaxItems` and `StartingToken` truncation. |
| `limit_key` | 3357 | str | No (missing in 184) | The request parameter for page size, e.g. `MaxResults`, `MaxKeys`, `Limit`. The `PaginationConfig.PageSize` setting is written into it. If it is absent, the operation has no page-size control. |
| `more_results` | 135 | str | No | A JMESPath expression for a boolean "more pages" flag, e.g. `IsTruncated` or `StreamDescription.HasMoreShards`. If present and falsy, pagination stops even when a token exists. Common in S3, CloudFront, Kinesis and other older APIs. |
| `non_aggregate_keys` | 81 (plus many more via `sdk-extras`) | list | No | JMESPath expressions for response fields that are **not** combined across pages, but should still appear in `build_full_result()`. They are metadata repeated on every page, like `Owner`, `Prefix`, `TotalHits`, `ColumnMetadata` or `RequestId`. Their values are captured from the pages and kept in the full result as single values, not concatenated. |

## Notable patterns

### Multiple tokens

Only 3 operations use list-valued tokens, and these are the composite cursors:

| Operation | `input_token` | `output_token` |
|---|---|---|
| `s3 ListMultipartUploads` | `KeyMarker`, `UploadIdMarker` | `NextKeyMarker`, `NextUploadIdMarker` |
| `s3 ListObjectVersions` | `KeyMarker`, `VersionIdMarker` | `NextKeyMarker`, `NextVersionIdMarker` |
| `route53 ListResourceRecordSets` | `StartRecordName`, `StartRecordType`, `StartRecordIdentifier` | `NextRecordName`, `NextRecordType`, `NextRecordIdentifier` |

These are also why the serialized resume token is a list-joined format.

### Computed `output_token`

Since `output_token` is JMESPath, some operations work out the token instead
of reading a field:

- `s3 ListObjects`: `NextMarker || Contents[-1].Key`. S3 only returns
  `NextMarker` when you send a delimiter, so otherwise the last key is used.
- `kinesis DescribeStream`: `StreamDescription.Shards[-1].ShardId`
- `importexport ListJobs`: `Jobs[-1].JobId`

### Nested paths

`output_token`, `result_key` and `more_results` can all be dotted paths into
wrapper structures. Examples:

- CloudFront: `DistributionList.Items` / `DistributionList.NextMarker` /
  `DistributionList.IsTruncated`
- RDS, ElastiCache, Neptune: `EngineDefaults.Marker` / `EngineDefaults.Parameters`
- Redshift: `DefaultClusterParameters.Marker`
- Athena: `ResultSet.Rows`

### Multiple result keys

Some operations return several parallel lists per page. For example:

- `ecr DescribeImageScanFindings`: `imageScanFindings.findings` and
  `imageScanFindings.enhancedFindings`
- `devops-guru GetResourceCollection`:
  `ResourceCollection.CloudFormation.StackNames` and `ResourceCollection.Tags`

### Questionable entries

A few `non_aggregate_keys` entries look like mistakes. For example,
`route53resolver` lists `MaxResults`, which is a request parameter echoed back.

## Keys that are not in the JSON

`PaginationConfig` (`MaxItems`, `PageSize`, `StartingToken`) and the internal
`boto_truncate_amount` resume-token field belong to the runtime API in
`paginate.py`. None of them appear in the data files.

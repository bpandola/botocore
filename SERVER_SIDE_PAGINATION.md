# Server-Side Pagination From Botocore Paginator Configs

This proposes a server-side counterpart to `botocore/paginate.py`. The server
takes the full result of an operation plus the request parameters, and
returns the single page that the client paginator expects. It reuses the
existing `paginators-1.json` configs (see `PAGINATOR_KEYS.md`) and adds a small
set of server-only keys.

There is a working prototype in `server_pagination.py`. Its tests drive it with
botocore's own `Paginator` class, covering `PageSize`, `MaxItems` and
`StartingToken` resume, and confirm that `build_full_result()` rebuilds the
original data exactly.

## 1. The contract the server must honour

These rules come from how `paginate.py` consumes pages:

| Client behaviour (`paginate.py`) | Server requirement |
|---|---|
| Stops when every `output_token` evaluates to a falsy value; empty strings count as `None`. | Leave out token fields on the last page. Never send `""` to mean "more". |
| Checks `more_results` first. If it is falsy, it stops even when a token is present. | When the config has `more_results`, always set it to `true` or `false`. |
| Raises `PaginationError` if it gets the same next token twice in a row. | Every token must move forward. A page must never be empty while `more_results` is `true`. |
| Writes `PageSize` into `limit_key`, converted to that member's modeled type (Route 53 `MaxItems` is a string). | Accept the limit as a string or an int, and apply min/max bounds. |
| `output_token` is JMESPath and can be computed, e.g. `NextMarker \|\| Contents[-1].Key`. | The server must emit the token so that the expression gives the right cursor. For `X[-1].Attr` forms, the cursor is the last item's attribute, so the server doesn't need to write it. |
| With several tokens, `output_token[i]` is paired with `input_token[i]`. When a token is `None`, its parameter is removed from the next request. | Composite cursors must allow trailing parts to be missing, e.g. Route 53 `StartRecordIdentifier`. |
| Only the **primary** (first) `result_key` counts toward `MaxItems`. The client slices into it when resuming from a `StartingToken` that has `boto_truncate_amount`. | Pages must be deterministic. Requesting the same token twice must return the same items, because client-side truncation re-requests a page and skips its first *n* items. |
| `build_full_result()` concatenates lists, **adds up** scalar result keys (DynamoDB `Count`), and takes `non_aggregate_keys` from the first page. | Scalar result keys hold *per-page* values. Non-aggregate fields must be the same on every page. |

## 2. Proposed additional configuration keys

Store these in a new `paginators-1.server.json` with the same `merge` format as
`paginators-1.sdk-extras.json`, so the client never loads them. They could also
go under a `"server"` sub-object of each operation entry. Most of them can be
derived automatically, which the "Default / derivation" column describes.

| Key | Type | Default / derivation | Purpose |
|---|---|---|---|
| `default_page_size` | int | `max_page_size`, else 100 | Page size used when the request leaves out `limit_key`. Also used for the 184 operations that have no `limit_key`. |
| `max_page_size` | int | The `max` trait of the `limit_key` member in `service-2.json` (present for about 81% of limit keys) | Upper bound on the page size. |
| `min_page_size` | int | The `min` trait of the `limit_key` member, else 1 | Lower bound on the page size. |
| `page_size_policy` | `"clamp"` \| `"error"` | `"clamp"` | What to do with an out-of-range limit: silently clamp it (S3 style) or reject it with a validation error. |
| `token_strategy` | `"opaque"` \| `"marker"` | `"marker"` if any `output_token` has the form `X[-1].Attr`, else `"opaque"` | **opaque**: a signed, server-encoded cursor (`NextToken`, `ContinuationToken`). **marker**: the token is a domain value taken from an item (S3 `Marker`, Kinesis `ExclusiveStartShardId`, DynamoDB `ExclusiveStartKey`). |
| `token_attributes` | list of `str` \| `list[str]` | The `Attr` from `X[-1].Attr` | For marker tokens, the item attribute behind each `input_token`. A nested list builds a map token, e.g. DynamoDB `LastEvaluatedKey` from the key schema. |
| `token_position` | `"after_last"` \| `"at_next"` | `"after_last"` | Whether the marker is the last item returned (exclusive start: S3, Kinesis, DynamoDB) or the first item of the next page (inclusive start: Route 53 `NextRecordName`). |
| `marker_match` | `"ordered"` \| `"exact"` | `"ordered"` | **ordered**: the items are sorted by the token attributes, and any value can be used as a marker (S3 `Marker` doesn't have to be an existing key). **exact**: the marker must match an item, and the server's own order is kept. Use this for orders that aren't lexical, like DynamoDB Scan or S3 object versions. |
| `bound_params` | list[str] | Every request member except tokens and limit (suggested) | Filter parameters that must stay the same across pages. Opaque tokens include a fingerprint of them, so a token replayed with different filters is rejected. |
| `token_ttl` | int seconds | none | Expiry time for opaque tokens. Some services model `ExpiredNextTokenException`. |
| `multi_result_mode` | `"primary"` \| `"parallel"` \| `"merged"` | `"primary"` | How to handle several `result_key`s. **primary**: page only the first and copy the others through. **parallel**: slice every list with the same window. **merged**: sort all lists together and share one page-size budget (S3 `Contents` + `CommonPrefixes` against `MaxKeys`). |
| `result_key_attributes` | map result_key → attribute | none | The sort and marker attribute for each list in `merged` mode, e.g. `{"Contents": "Key", "CommonPrefixes": "Prefix"}`. |
| `count_keys` | map result_key → list result_key | none | Scalar result keys calculated per page from a list's length (DynamoDB `Count`/`ScannedCount` → `Items`). The client adds these up across pages. |
| `echo_params` | list[str] | none | Request parameters copied into the response (S3 echoes `Marker`, `MaxKeys` and `Prefix`). |
| `invalid_token_error` | `{code, message}` | The first error shape on the operation whose name contains "Token" or "Paginat", such as `InvalidNextTokenException` (234 operations), `InvalidNextToken` (50) or `InvalidPaginationTokenException` (36). Otherwise `InvalidNextTokenException`. | The error returned for a malformed, tampered, expired or mismatched token. |
| `invalid_page_size_error` | `{code, message}` | `ValidationException` | The error returned when `page_size_policy` is `"error"`. |

### Example server config file

```json
{
  "version": 1.0,
  "merge": {
    "pagination": {
      "ListObjects": {
        "multi_result_mode": "merged",
        "result_key_attributes": {"Contents": "Key", "CommonPrefixes": "Prefix"},
        "echo_params": ["Marker", "MaxKeys", "Prefix", "Delimiter"]
      },
      "ListObjectsV2": {
        "multi_result_mode": "merged",
        "result_key_attributes": {"Contents": "Key", "CommonPrefixes": "Prefix"},
        "bound_params": ["Bucket", "Prefix", "Delimiter"]
      },
      "ListParts": {
        "token_strategy": "marker",
        "token_attributes": ["PartNumber"]
      }
    }
  }
}
```

```json
{"merge": {"pagination": {
  "ListResourceRecordSets": {
    "token_strategy": "marker",
    "token_attributes": ["Name", "Type", "SetIdentifier"],
    "token_position": "at_next"
  }}}}
```

```json
{"merge": {"pagination": {
  "Scan": {
    "token_strategy": "marker",
    "token_attributes": [["pk", "sk"]],
    "marker_match": "exact",
    "count_keys": {"Count": "Items", "ScannedCount": "Items"}
  }}}}
```

Kinesis `DescribeStream` and CloudFront's `List*` operations need **no** server
config. Their marker attribute and nested paths are inferred from the client
config.

## 3. Classes

All of these are in `server_pagination.py`.

| Class | Role |
|---|---|
| `ServerPaginationConfig` | Combines a client config entry, the optional server keys and, optionally, an `OperationModel`. Validates the keys and fills in derived defaults. `from_botocore(op, client_cfg, server_cfg, operation_model)` is the main constructor. It builds without errors for every one of the ~3,500 shipped configs. |
| `OutputTokenSpec` | Parses one `output_token` JMESPath into a **write path** (a plain dotted field such as `DistributionList.NextMarker`) and/or a **derived attribute** (`Contents[-1].Key` → `Key`). For `A \|\| B[-1].C`, the server writes `A`, and `C` is used as the marker attribute. Any other expression shapes are rejected. |
| `OpaqueTokenCodec` | Encodes `{offset, bound-param fingerprint, expiry}` as base64url JSON with a truncated HMAC-SHA256. It rejects tokens that are tampered with, expired, or used with different filters. |
| `ServerPaginator` | `paginate(full_response, params) -> page`. |
| `PaginationServerError` | Holds `code`, `message` and `status_code`, so a framework can turn it into the service's error format. |

### `ServerPaginator.paginate` flow

1. **Page size**: read `params[limit_key]`, convert it to int, then clamp it or reject it.
2. **Entries**: flatten the result list(s) into `(sort_key, result_key, item)` tuples.
   Merged mode puts several lists into one sorted stream. Ordered markers sort by the token attributes.
3. **Start position**: no token means 0. Opaque tokens are decoded to an offset.
   Ordered markers use `bisect_right` (after_last) or `bisect_left` (at_next).
   Exact markers use the index of the matching item.
4. **Window**: `entries[start:start + page_size]`. The page is truncated if more entries come after it.
5. **Build the page**: deep-copy the full response, so the non-aggregate fields are
   kept as they are. Replace each result path with its slice using
   `botocore.utils.set_value_from_jmespath`, which also handles nested paths.
   Recalculate `count_keys` and remove any old token fields.
6. **Tokens**: if truncated, write each output token to its write path. Opaque tokens
   store the next offset. Markers come from the last item (`after_last`) or the
   next item (`at_next`). Set `more_results` and copy over `echo_params`.

### Usage

```python
config = ServerPaginationConfig.from_botocore(
    'ListAttachedLinks',
    paginators['pagination']['ListAttachedLinks'],
    server_config={'bound_params': ['SinkIdentifier']},
    operation_model=service_model.operation_model('ListAttachedLinks'),
)
paginator = ServerPaginator(config, token_secret=SECRET)

def list_attached_links(**params):
    full = {'Items': backend.links_for(params['SinkIdentifier'])}
    return paginator.paginate(full, params)
```

## 4. Validation

The prototype's tests (`tests/functional/test_server_pagination.py`) connect `ServerPaginator` to `botocore.paginate.Paginator`
as the operation method, then check the results of `build_full_result()`. These
cases pass:

- Opaque `NextToken` with page sizes 1 to 1000; `MaxItems` + `StartingToken`
  resume, including the mid-page `boto_truncate_amount` path; bound-param
  mismatch and garbage tokens are rejected; page bounds and the error code are
  derived from the OAM service model.
- S3 `ListObjects`: merged `Contents`/`CommonPrefixes` sharing `MaxKeys`, and
  the `NextMarker || Contents[-1].Key` token.
- S3 `ListObjectsV2`: opaque token in merged mode. S3 `ListParts`: integer marker
  and non-aggregate `Owner`.
- Route 53 `ListResourceRecordSets`: three-part inclusive marker with an
  optional `SetIdentifier`, using a string-typed `MaxItems`.
- Kinesis `DescribeStream`: derived token, nested `StreamDescription.Shards`,
  `HasMoreShards`.
- CloudFront `ListDistributions`: nested `DistributionList.*` tokens and `IsTruncated`.
- DynamoDB `Scan`: map token, exact matching, summed `Count`/`ScannedCount`.

## 5. Limitations and next steps

- **Data that changes between pages.** Offset-based opaque tokens can skip or
  repeat items if the data changes between requests. Production code should
  encode the last item's sort key in the opaque token (keyset pagination)
  instead of an offset. The codec already has room for that.
- **Loading everything.** `paginate()` takes the whole result set. For large
  backends, add a `PageSource` protocol, e.g. `fetch(start_after, limit + 1)`,
  so the paginator asks the data store for one page plus one extra item to
  detect truncation.
- **Opaque with several tokens.** It isn't supported yet. No shipped config needs it.
- **`parallel` mode** assumes the lists line up item by item. Check this for
  each operation (e.g. ECR `findings`/`enhancedFindings`).
- **Errors in `non_aggregate_keys`.** Some entries look wrong, such as
  route53resolver's `MaxResults`. The server can't fix these, but a check could
  flag non-aggregate keys that are also request members.
- **Generating defaults.** A script could create a first
  `paginators-1.server.json` for every service, using the derivations above,
  and flag operations that need a human decision (several result keys, marker
  semantics).

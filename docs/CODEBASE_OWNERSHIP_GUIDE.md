# Hướng dẫn nghiệm thu và làm chủ codebase

Tài liệu này giúp bạn đạt ba mục tiêu:

1. Hiểu hệ thống chạy từ đầu đến cuối và biết đoạn code nào sở hữu từng hành vi.
2. Hiểu cách project triển khai các kỹ thuật data engineering như incremental,
   idempotency, retry, rerun, backfill, quarantine và reconciliation.
3. Có thể tự vận hành, điều tra lỗi và thay đổi code an toàn mà không phụ thuộc
   vào người viết ban đầu.

Hãy đọc tài liệu song song với code. Không cần học thuộc. Một owner tốt là
người biết tìm câu trả lời đúng từ code, runtime state và bằng chứng đã lưu.

## 1. Cách học nhanh với tài liệu này

Học theo ba vòng, không đọc tuần tự toàn bộ một lần.

### Vòng 1 — Nắm luồng trong 30 phút

Đọc các phần:

- Mô hình tư duy.
- Luồng chạy end-to-end.
- Bản giải thích hệ thống trong một phút.

Mục tiêu: tự vẽ lại được pipeline và nói đúng vai trò của từng service.

### Vòng 2 — Hiểu các cơ chế cốt lõi

Đọc các phần:

- Source và row identity.
- Bronze, Silver và quarantine.
- Incremental, idempotency, retry, rerun và backfill.
- Reconciliation, publication và verification.

Mục tiêu: giải thích được vì sao pipeline chạy lại không nhân đôi dữ liệu và
vì sao output chỉ được publish khi đủ bằng chứng.

### Vòng 3 — Trở thành owner

Thực hiện chương trình bảy buổi, trả lời bộ câu hỏi nghiệm thu và hoàn thành
checklist ở cuối tài liệu.

Mục tiêu: có thể tự vận hành và review một thay đổi thật.

## 2. Owner của codebase phải làm được gì?

### Giải thích được hệ thống

- Vẽ được đường đi của dữ liệu từ S3 landing đến Redshift Gold.
- Nêu đúng service nào sở hữu orchestration, compute, storage, metadata,
  modeling, serving và release evidence.
- Trace được một monthly run từ input đến output.
- Phân biệt source identity, `row_id`, `business_trip_key`, Airflow DAG run ID
  và `ingestion_run_id`.
- Giải thích được vì sao reconciliation, publication và verification là ba
  boundary riêng.

### Vận hành được hệ thống

- Chạy toàn bộ local gates và xác định code chịu trách nhiệm khi có lỗi.
- Deploy bằng đúng Terraform state và reviewed plan.
- Trigger một tháng hoặc backfill, rồi tìm được Airflow task, EMR job, Iceberg
  snapshot, manifest, dbt artifact và publication tương ứng.
- Retry an toàn mà không xóa state, sửa dữ liệu canonical hoặc bypass gate.

### Thay đổi code an toàn

- Xác định các contract bị ảnh hưởng trước khi sửa source field, identity rule,
  quality rule, schema, dbt model hoặc Terraform.
- Thêm test nhỏ nhất chứng minh behavior mới và chạy các test lân cận.
- Nói rõ thay đổi có backward-compatible không, có cần migration không, có làm
  thay đổi dữ liệu lịch sử hoặc publication đã lưu không.

## 3. Mô hình tư duy: ba plane

```text
CONTROL PLANE
Amazon MWAA / Airflow
    chuẩn bị tháng, sắp thứ tự task, retry và điều phối backfill

DATA PLANE
S3 landing -> EMR Spark -> Iceberg Bronze/Silver/Q -> Spectrum -> Redshift Gold

EVIDENCE PLANE
ops.source_run_manifest -> dbt run_results -> reconciliation
                        -> publication JSON -> read-after-publish verification
```

Mỗi plane trả lời một câu hỏi khác nhau:

| Plane | Câu hỏi |
| --- | --- |
| Control | Bước nào phải chạy tiếp theo và task đã hoàn tất chưa? |
| Data | Record nào đang tồn tại trong từng layer? |
| Evidence | Source và output chính xác nào đã được chấp nhận thành một release? |

Airflow báo success chưa đủ để khẳng định dữ liệu đã được phát hành. Một release
chỉ tồn tại sau khi evidence plane đã reconcile, publish và verify thành công.

### Ownership của kiến trúc

| Trách nhiệm | Owner chính | Không phải owner |
| --- | --- | --- |
| Source đã land | Upstream producer và S3 | Repo không upload source |
| Thứ tự workflow và retry | MWAA / Airflow | Spark và dbt không tự gọi nhau |
| Batch transformation | transient EMR on EC2 / Spark | Primary On-Demand, Core Spot; Glue không chạy ETL trong project này |
| Dữ liệu và snapshot của open layers | S3 / Iceberg | Redshift không sở hữu Bronze/Silver |
| Metadata của open layers | Glue Data Catalog | Glue không transform dữ liệu |
| Business SQL và serving | dbt + Redshift Gold | Iceberg không sở hữu Gold |
| Truy vấn open layers | Redshift Spectrum | Không có Athena path |
| Operational state có thể cập nhật | `ops.source_run_manifest` | Publication JSON là immutable evidence |
| Release được chấp nhận | Publication JSON trên S3 | Task success chưa đủ |

## 4. Luồng chạy end-to-end

Điểm bắt đầu tốt nhất là
[nyc_hvfhs_monthly_dag.py](../etl/dags/nyc_hvfhs_monthly_dag.py).

Thứ tự bắt buộc của DAG:

```text
prepare_month
-> bronze_ingestion_emr
-> silver_transform_emr
-> dbt_build
-> dbt_result_artifact
-> reconciliation
-> publication_manifest
-> verification
```

### Bản đồ A-to-Z

| Bước | Operation | Code sở hữu | Kết quả bền vững |
| --- | --- | --- | --- |
| 1 | Resolve và validate tháng | `_prepare_month`, `_s3_identity` trong DAG | XCom audit chứa source identity và stable run ID |
| 2 | Submit Spark job có cấu hình Iceberg | `_emr_spark_job` trong DAG | EMR job run và log S3 |
| 3 | Validate source, ghi Bronze | `nyc_bronze_ingestion.py` | Bronze partition, snapshot, manifest state |
| 4 | Validate, deduplicate, tạo Silver và quarantine | `nyc_silver_transform.py` | Silver/Q partitions, snapshots, manifest state |
| 5 | Build và test Gold | Cosmos `DbtTaskGroup`, `etl/dbt_project` | Sáu Redshift relations và `run_results.json` |
| 6 | Lưu bằng chứng dbt | `nyc_hvfhs_cosmos.py` | dbt artifact có checksum trên S3 |
| 7 | Đối chiếu các layer | `nyc_hvfhs_reconciliation.py` | Counts, snapshots và Redshift statement ID |
| 8 | Tạo hoặc reuse release | `nyc_hvfhs_publication.py`, `publication/nyc_hvfhs.py` | Publication JSON có checksum |
| 9 | Đọc lại như consumer | `nyc_hvfhs_verification.py` | Read-after-publish result |

## 5. Bước 1 — Chuẩn bị tháng và khóa source identity

Trong DAG, `_prepare_month` nhận đúng hai input: `year` và `month`.

Nó thực hiện:

1. Tạo key `landing/fhvhv_tripdata_YYYY-MM.parquet`.
2. Gọi S3 `HeadObject` cho trip file và Taxi Zone file.
3. Bắt buộc object không rỗng và có metadata `sha256` viết thường.
4. Kiểm tra URI và filename bằng `validate_landed_source`.
5. Tạo `ingestion_run_id` ổn định bằng `stable_run_id`.
6. Trả audit object qua XCom cho toàn bộ downstream tasks.

Pure source contract nằm trong
[sources/nyc_hvfhs.py](../etl/sources/nyc_hvfhs.py). S3 call nằm trong DAG,
còn validation thuần Python nằm trong source module để có thể test không cần AWS.

### Source identity là gì?

```text
source URI + SHA-256 + byte size + year + month
```

`stable_run_id` là hash xác định từ năm thành phần này. Vì vậy:

- Hai Airflow DAG runs khác nhau có thể có cùng `ingestion_run_id`.
- Cùng một immutable source luôn tạo cùng `ingestion_run_id`.
- URI, checksum hoặc size thay đổi sẽ tạo identity khác và bị manifest guard
  chặn nếu tháng đó đã tồn tại.

### Vì sao Bronze kiểm tra object thêm một lần?

Có khoảng thời gian giữa lúc Airflow chuẩn bị request và lúc EMR thật sự đọc
file. Compute boundary phải tự xác minh object chưa thay đổi trước khi ghi
canonical data. Đây là validation tại trust boundary, không phải code lặp vô ích.

## 6. Bước 2 — Bronze ingestion

Runtime code:
[nyc_bronze_ingestion.py](../etl/spark_jobs/nyc_bronze_ingestion.py).

Luồng chính:

1. Parse các job arguments tường minh.
2. Tính lại stable run ID và từ chối nếu không khớp.
3. Ở lần chạy đầu, tạo Iceberg namespaces và tables từ
   [catalog.py](../etl/iceberg/catalog.py).
4. Kiểm tra lại S3 checksum và object size.
5. Load Taxi Zones nếu reference table đang rỗng.
6. Đọc `ops.source_run_manifest` và khóa identity của tháng.
7. Đọc Parquet, kiểm tra schema bắt buộc.
8. Gắn lineage metadata, `row_id` và `business_trip_key`.
9. Từ chối input rỗng.
10. Replace đúng partition tháng đang xử lý.
11. Lấy Iceberg snapshot ID vừa commit.
12. Ghi manifest status `bronze_published`.

Bronze giữ nguyên source fields và chỉ thêm lineage/identity metadata. Bronze
không lọc invalid row và không xóa duplicate. Grain của Bronze là một source
row đã land, kể cả row lỗi hoặc trùng.

### Taxi Zone reference

Reference table chỉ được load khi đang rỗng. Nếu đã có dữ liệu:

- Cùng checksum: reuse.
- Khác checksum: fail và yêu cầu migration tường minh.

Reference data không bị âm thầm overwrite trong mỗi monthly run.

## 7. Row identity và deduplication

Single source of truth là
[nyc_hvfhs_identity.py](../etl/contracts/nyc_hvfhs_identity.py).

### `row_id`

`row_id` hash:

```text
identity policy version + toàn bộ canonical source fields theo đúng thứ tự
```

Nó dùng cho:

- Exact duplicate detection trong Silver.
- `unique_key` của dbt incremental merge trong `fct_trips`.

Hai row chỉ có cùng `row_id` khi toàn bộ nội dung thuộc identity policy giống
nhau sau canonicalization.

### `business_trip_key`

`business_trip_key` chỉ hash một số trip descriptors chính. Nó có thể nhóm các
chuyến đi có vẻ là cùng một real-world trip, nhưng không đủ chặt để xóa row.

Nó chỉ phục vụ phân tích. Nó không được dùng để deduplicate, merge hoặc
quarantine.

### Vì sao cần canonicalization?

Cùng một giá trị logic có thể có nhiều representation. Code chuẩn hóa:

- Timestamp về cùng format.
- Integer bỏ formatting thừa.
- Numeric về sáu chữ số thập phân.
- `NULL` và blank thành token `<NULL>`.
- Các field được nối bằng separator cố định.

Python và Spark phải tạo cùng byte sequence. Golden-vector tests và Spark
parity test bảo vệ contract này.

### Identity policy 2024 và 2025

Từ 2025, `cbd_congestion_fee` tham gia exact identity nên policy version thay
đổi. Row lịch sử 2024 vẫn giữ policy và `row_id` cũ.

## 8. Bước 3 — Silver và quarantine

Runtime code:
[nyc_silver_transform.py](../etl/spark_jobs/nyc_silver_transform.py).

Silver chỉ chạy khi manifest có:

- `bronze_published`; hoặc
- `failed` với `failure_stage = silver`, tức là đang retry Silver.

Nó đọc đúng `year + month + ingestion_run_id` từ Bronze rồi:

1. Xác minh identity policy.
2. Join Taxi Zones hai lần để kiểm tra pickup/drop-off zone.
3. Áp dụng quality rules theo thứ tự trong
   [nyc_hvfhs_quality.py](../etl/contracts/nyc_hvfhs_quality.py).
4. Đánh số các row có cùng `row_id` theo thứ tự xác định.
5. Giữ valid exact row đầu tiên trong Silver.
6. Đưa exact duplicates tiếp theo vào quarantine.
7. Tạo typed fields, trip duration, pickup date và pickup hour.
8. Tính count từ classified frame đã được persist.
9. Replace partition tháng của Silver và quarantine.
10. Lưu hai snapshot IDs và chuyển manifest thành `silver_published`.

### Deterministic quarantine

Mỗi row chỉ nhận reason đầu tiên phù hợp. Priority hiện tại là:

```text
timestamp
-> timeline
-> zone ID / zone lookup
-> numeric validity
-> negative values
-> exact duplicate
```

Duplicate đứng cuối. Nếu một duplicate cũng có timestamp lỗi, quarantine hiển
thị lỗi timestamp vì đó là nguyên nhân hữu ích hơn.

Thứ tự ổn định giúp rerun tạo cùng reason distribution.

### Invariant đầu tiên

```text
Bronze count = Silver count + quarantine count
```

Mọi Bronze row phải được giải thích. Không có silent drop.

## 9. Iceberg và Glue Data Catalog

[catalog.py](../etl/iceberg/catalog.py) định nghĩa năm bảng:

```text
bronze.bronze_hvfhs_trips
bronze.bronze_taxi_zones
silver.silver_trips
silver.quarantine_trips
ops.source_run_manifest
```

### Phân vai

- S3 lưu data files và Iceberg metadata files.
- Iceberg cung cấp atomic commit, snapshot, schema evolution và time travel.
- Glue Data Catalog lưu shared table definitions để EMR và Redshift cùng tìm
  được bảng.
- Glue không chạy ETL trong project này.

Trip, quarantine và manifest tables được partition theo source year/month.
Taxi Zones nhỏ và không partition.

`overwritePartitions()` thay đúng partition xuất hiện trong DataFrame, không
overwrite toàn bảng. Monthly partition phù hợp với ingestion/rerun grain.

Project đặt `max_active_runs=1`, vì vậy chưa tuyên bố đã chứng minh concurrent
writes cho nhiều tháng.

## 10. Bước 4 — Cosmos, dbt và Redshift Gold

DAG dùng Cosmos `DbtTaskGroup` với Watcher mode. Cosmos:

- Chạy một `dbt build`.
- Hiển thị trạng thái model/test trong Airflow graph.
- Giữ dependency graph của dbt trong orchestration.
- Cho phép downstream task chỉ chạy sau khi dbt graph thành công.

Đây không phải `BashOperator` bọc quanh dbt.

MWAA cài Cosmos trong Airflow environment. Script
[mwaa_startup.sh](../scripts/mwaa_startup.sh) tạo dbt virtualenv riêng vì
dependency constraints của Airflow và dbt xung đột. DAG gọi rõ binary:

```text
/usr/local/airflow/dbt_venv/bin/dbt
```

[profiles.yml](../etl/dbt_project/profiles.yml) kết nối private Redshift
Serverless bằng IAM-role authentication. dbt đọc:

- `bronze_external`: Spectrum mapping tới Glue Bronze database.
- `silver_external`: Spectrum mapping tới Glue Silver database.

### dbt graph

```text
bronze_external.bronze_taxi_zones -> dim_zone

silver_external.silver_trips -> dim_date
                             -> dim_operator
                             -> fct_trips -> mart_hourly_zone_demand
                                          -> mart_operator_metrics

fct_trips -- relationship tests --> dim_date / dim_operator / dim_zone
```

### Grain và materialization

| Model | Grain | Materialization |
| --- | --- | --- |
| `dim_date` | Một row mỗi pickup date | Table rebuild |
| `dim_operator` | Một row mỗi operator code | Table rebuild |
| `dim_zone` | Một row mỗi Taxi Zone | Table rebuild |
| `fct_trips` | Một row mỗi valid, deduplicated `row_id` | Incremental merge |
| `mart_hourly_zone_demand` | Một row mỗi month/date/hour/pickup zone | Table rebuild |
| `mart_operator_metrics` | Một row mỗi year/month/operator | Table rebuild |

Chỉ `fct_trips` là dbt incremental model. Nó đọc tháng được truyền qua dbt vars
và merge vào Redshift bằng `unique_key='row_id'`.

Dimensions và marts nhỏ nên được rebuild toàn bảng để implementation dễ hiểu và
ít state hơn. Không được nói rằng toàn bộ Gold đều incremental.

### dbt tests kiểm tra gì?

- Source keys unique/not-null.
- Model keys unique/not-null.
- Fact foreign keys tồn tại trong dimensions.
- Các measure quan trọng không null.
- Tổng fact count bằng toàn bộ Silver count.

dbt tests chưa thay thế Airflow reconciliation. dbt kiểm tra model graph;
reconciliation gắn một monthly source run với manifest, snapshots và counts
thực sự nhìn thấy từ consumer query plane.

## 11. Bước 5 — dbt artifact

[nyc_hvfhs_cosmos.py](../etl/orchestration/nyc_hvfhs_cosmos.py) xử lý
`run_results.json`:

1. Bắt buộc có ít nhất một result.
2. Bắt buộc có `metadata.invocation_id`.
3. Mọi status phải là `success` hoặc `pass`.
4. Ghi artifact lên S3 dưới stable run ID.
5. Ghi SHA-256 vào S3 metadata.

Nếu artifact hợp lệ đã tồn tại, code xác minh checksum rồi reuse. Vì vậy
identical rerun giữ lại first successful dbt artifact.

Artifact này trả lời: “dbt graph nào đã chạy thành công cho source run này?”

## 12. Bước 6 — Reconciliation

[nyc_hvfhs_reconciliation.py](../etl/orchestration/nyc_hvfhs_reconciliation.py)
chạy một parameterized query qua Redshift Data API.

Query đọc trong cùng Redshift consumer plane:

- Bronze qua Spectrum.
- Silver qua Spectrum.
- Quarantine qua Spectrum.
- Operational manifest qua Spectrum.
- `gold.fct_trips` trong Redshift.

Nó bắt buộc:

```text
manifest counts = Spectrum-visible counts
Bronze = Silver + quarantine
Silver = Gold fct_trips
đủ Bronze, Silver và quarantine snapshot IDs
```

Reconciliation trả lời: “Các durable layers có thật sự cân bằng sau khi ghi
không?”

## 13. Bước 7 — Publication

Publication có hai phần:

- Pure document builder:
  [publication/nyc_hvfhs.py](../etl/publication/nyc_hvfhs.py).
- AWS/S3 adapter:
  [nyc_hvfhs_publication.py](../etl/orchestration/nyc_hvfhs_publication.py).

Publication JSON chứa:

- Immutable source identity.
- Stable ingestion run ID.
- Identity policy version.
- Ba Iceberg table identifiers, snapshots và counts.
- Redshift database/schema và đúng sáu Gold relations.
- Reconciliation result.
- dbt artifact URI và checksum.
- Publication timestamp.

### Idempotent publication

Publication key dựa trên `year/month/run_id`.

Nếu key đã tồn tại:

- Logical content giống nhau: reuse object cũ.
- Logical content khác: fail như integrity incident.

Khi so sánh logical content, code bỏ attempt-specific fields như timestamp và
Redshift statement ID. Nó không overwrite conflicting release.

Publication trả lời: “Phiên bản dữ liệu chính xác nào đã được chấp nhận để
phục vụ?”

## 14. Bước 8 — Verification

[nyc_hvfhs_verification.py](../etl/orchestration/nyc_hvfhs_verification.py):

1. Download publication JSON.
2. Xác minh SHA-256.
3. Xác minh source month và stable run ID.
4. Query lại Silver qua Spectrum.
5. Query lại Gold fact trong Redshift.
6. So sánh các count đọc được với publication.

Verification trả lời: “Sau khi publish, consumer có đọc được đúng output mà
release tuyên bố không?”

Đó là lý do verification không được gộp với reconciliation.

## 15. Incremental processing

Project không dùng cùng một kiểu incremental cho mọi layer.

### Bronze và Silver

- Đơn vị xử lý là một tháng.
- Chỉ đọc requested month/run.
- Replace đúng Iceberg partition tháng đó.

### Gold fact

- dbt chỉ select requested source year/month từ Silver.
- Redshift merge vào `fct_trips` theo `row_id`.

### Dimensions và marts

- Rebuild từ toàn bộ dữ liệu hiện có.
- Đây là lựa chọn đơn giản phù hợp với kích thước bounded của project.

Incremental giúp giảm lượng dữ liệu phải xử lý. Nó chưa tự bảo đảm idempotency;
nếu key hoặc write strategy sai, pipeline vẫn có thể tạo duplicate.

## 16. Idempotency

Idempotency nghĩa là chạy lại cùng logical request sẽ hội tụ về cùng canonical
result.

Trong project này, nó là kết quả phối hợp của nhiều cơ chế:

```text
immutable source identity
+ stable ingestion_run_id
+ month-scoped Iceberg partition replacement
+ exact row_id deduplication
+ dbt merge theo row_id
+ immutable/reusable publication key
= safe identical-source rerun
```

Không có một helper đơn lẻ nào “tạo ra idempotency” cho toàn hệ thống.

## 17. Retry, task clear, rerun, backfill khác nhau thế nào?

| Cơ chế | Ý nghĩa | Implementation |
| --- | --- | --- |
| Task retry | Airflow thử lại task lỗi trong cùng DAG run | `retries=2`, delay 5 phút |
| Task clear | Operator chủ động chạy lại một task đã chọn | Airflow UI/API; durable state vẫn còn |
| Monthly rerun | Tạo DAG run mới cho cùng immutable month | Stable identity và guarded state |
| Backfill | Chạy lại nhiều historical monthly units | Bốn `TriggerDagRunOperator` tuần tự |
| Incremental model | Chỉ transform delta được yêu cầu | Spark month partitions và dbt fact merge |

### Identical monthly rerun thực sự làm gì?

1. `prepare_month` tạo lại cùng run ID.
2. Bronze xác minh identity; nếu manifest đã `silver_published` thì không rewrite.
3. Silver thấy `silver_published` và không rewrite.
4. dbt build/test Gold lại.
5. dbt artifact đầu tiên được reuse.
6. Reconciliation chạy lại.
7. Publication được so sánh và reuse.
8. Verification chạy lại.

Rerun không “skip tất cả”. Open layers được reuse nhưng serving và release
gates vẫn được xác minh lại.

## 18. Backfill

`nyc_hvfhs_four_month_backfill`:

1. Nhận start year/month.
2. Tạo đúng bốn month requests.
3. Trigger monthly DAG cho từng tháng.
4. Chờ tháng hiện tại hoàn thành trước khi chạy tháng tiếp theo.

Backfill không có transformation logic riêng; nó reuse production monthly DAG.
Đây là best practice quan trọng vì tránh hai code paths xử lý cùng dữ liệu.

Giới hạn hiện tại:

- Đúng bốn tháng.
- Không được bắt đầu sau tháng 9 vì không cross year.
- Chạy tuần tự, không tăng concurrency.

Đây là bounded proof, không phải generic enterprise backfill framework.

## 19. Reconciliation khác testing thế nào?

Tests kiểm tra logic hoặc contract có đúng không. Reconciliation kiểm tra các
durable layers sau khi hệ thống đã thật sự ghi dữ liệu có đồng ý với nhau không.

Hai count equations đơn giản nhưng mạnh cho data product này:

```text
Bronze = Silver + quarantine
Silver = Gold fact
```

Count khớp chưa chứng minh mọi field đều đúng. Vì vậy hệ thống vẫn cần source
contract, identity, quality rules, dbt tests và snapshot evidence.

## 20. Schema evolution và time travel

Thay đổi duy nhất đã được approve là nullable `cbd_congestion_fee` cho 2025.

[apply_nyc_2025_schema_evolution.py](../etl/spark_jobs/apply_nyc_2025_schema_evolution.py)
thêm cột vào:

- Bronze trips.
- Silver trips.
- Quarantine trips.

Đồng thời:

- 2025 source contract yêu cầu field này.
- 2025 exact identity policy bao gồm field này.
- dbt trả `NULL` cho dữ liệu trước 2025.

[verify_nyc_snapshot.py](../etl/spark_jobs/verify_nyc_snapshot.py) đọc retained
2024 Silver snapshot bằng `VERSION AS OF` sau khi current schema đã đổi.

Schema evolution chỉ được nghiệm thu khi historical data vẫn đọc được.

Không có generic migration framework. Mỗi schema change mới phải bắt đầu bằng
compatibility decision tường minh.

## 21. Runtime state machine

```text
no row
  |
  v
bronze_published -> silver_published
       ^                  ^
       |                  |
failed(stage=bronze)      failed(stage=silver)
       |                  |
       +-- retry Bronze   +-- retry Silver
```

Publication không phải manifest state. Nó là immutable release object riêng,
được tạo sau Gold và reconciliation.

### Failure và safe recovery

| Failure point | Durable state có thể còn | Cách recovery an toàn |
| --- | --- | --- |
| S3 object thiếu/sai | Chưa có accepted mutation | Sửa producer contract rồi chạy lại |
| Bronze trước commit | Tables có thể đã tạo, month chưa publish | Sửa nguyên nhân và retry Bronze |
| Bronze sau commit | Bronze snapshot/manifest có thể đã tồn tại | Retry; partition replacement và manifest merge xử lý lại |
| Silver write | Bronze còn nguyên; có thể đã có một open-layer commit | Sửa nguyên nhân và retry Silver |
| dbt model/test | Silver đã publish, chưa có release | Sửa model/data rồi retry dbt |
| dbt artifact | Gold có thể tồn tại, release gate vẫn đóng | Sửa S3/artifact và chạy lại stage |
| Reconciliation | Các layer không khớp, chưa publication | Tìm layer đầu tiên sai và rerun canonical owner |
| Publication conflict | Key cũ có logical content khác | Xử lý như integrity incident, không overwrite |
| Verification | Publication tồn tại nhưng consumer read sai | Sửa visibility/serving rồi rerun verification |

Không recovery bằng cách:

- Sửa object đã được accept.
- Xóa Terraform state.
- Manually edit canonical table.
- Skip release gate.
- Thêm `force=true`.

## 22. Bản đồ code

| Path | Trách nhiệm chính |
| --- | --- |
| `etl/dags/nyc_hvfhs_monthly_dag.py` | Runtime order và service hand-offs |
| `etl/sources/nyc_hvfhs.py` | Source filename, schema và stable run identity |
| `etl/contracts/nyc_hvfhs_identity.py` | Exact/probable identity cho Python và Spark |
| `etl/contracts/nyc_hvfhs_quality.py` | Ordered quarantine policy |
| `etl/iceberg/catalog.py` | Open-table schemas và partitions |
| `etl/spark_jobs/nyc_bronze_ingestion.py` | Source trust boundary và Bronze state |
| `etl/spark_jobs/nyc_silver_transform.py` | Classification, dedup và Silver state |
| `etl/dbt_project` | Gold graph, grains, materializations và tests |
| `etl/orchestration/nyc_hvfhs_cosmos.py` | Durable dbt evidence |
| `etl/orchestration/nyc_hvfhs_reconciliation.py` | Cross-layer invariants |
| `etl/publication/nyc_hvfhs.py` | Pure publication contract |
| `etl/orchestration/nyc_hvfhs_publication.py` | S3 publication adapter |
| `etl/orchestration/nyc_hvfhs_verification.py` | Consumer read-after-publish check |
| `terraform` | AWS services, IAM, network attachment và bootstrap |

## 23. Change-impact map

| Thay đổi | Tối thiểu phải kiểm tra |
| --- | --- |
| Thêm/bỏ source field | Source contract, identity decision, Iceberg specs, Bronze/Silver, fixtures |
| Đổi exact dedup | Python/Spark identity, golden vectors, Silver window, dbt key, rerun evidence |
| Thêm quality rule | Python/Spark reason policy, priority tests, quarantine history |
| Thêm Silver derived field | Iceberg schema, Spark select, Spectrum, dbt source/models/tests |
| Thêm Gold measure | dbt model, grain, schema tests, consumer contract |
| Thêm/bỏ Gold relation | dbt graph, required relations, publication, docs và tests |
| Đổi orchestration | DAG topology, retry/rerun, MWAA packaging, IAM |
| Đổi AWS service boundary | Terraform, IAM, network, runbook, cost, teardown |
| Đổi publication | Builder, logical comparison, verifier, retained evidence compatibility |

Nguyên tắc: sửa canonical owner trước, sau đó sửa direct consumers và contract
tests. Không vá cùng một symptom ở nhiều downstream modules.

## 24. Terraform và infrastructure ownership

| File | Sở hữu |
| --- | --- |
| `s3.tf` | Private, versioned, encrypted, non-force-destroy storage |
| `glue_catalog.tf` | Bronze, Silver, Ops metadata namespaces |
| `emr_ec2.tf` | Runtime artifacts, EMR service role và EC2 instance profile |
| `mwaa.tf` | Airflow, DAG sync, Cosmos/dbt startup, logs, network attachment |
| `redshift_serverless.tf` | Private workgroup, Spectrum, external schemas, Gold grants |
| `iam.tf` | EMR, MWAA và Redshift role boundaries |
| `variables.tf` | Validated deployment inputs và cost bounds |
| `main.tf` | Provider, outputs và non-secret Airflow Variables |

### Các trust boundary cần nhớ

- Repo dùng VPC và hai private subnets có sẵn; không tạo NAT/VPC endpoints.
- MWAA dùng execution role, không dùng static AWS keys.
- Redshift private, chỉ nhận port 5439 từ MWAA security group.
- MWAA chỉ start/observe EMR jobs và pass đúng EMR execution role.
- EMR mutate warehouse/log prefixes thuộc trách nhiệm của nó.
- Redshift Spectrum role chỉ đọc open layers.
- S3 versioning, encryption, public block và `force_destroy=false` giảm nguy cơ
  mất dữ liệu.

Deploy theo [RUNBOOK.md](RUNBOOK.md). `terraform validate` không chứng minh
quota, network egress, IAM runtime hoặc cross-service integration thực tế.

## 25. Test suite bảo vệ gì?

| Test | Contract được bảo vệ |
| --- | --- |
| `test_nyc_hvfhs_source.py` | Source URI/checksum/size/schema và stable run ID |
| `test_nyc_identity_contract.py` | Golden hashes và Python/Spark parity |
| `test_nyc_quality_priority.py` | First-reason priority và invalid numeric |
| `test_nyc_hvfhs_transform.py` | Source-faithful Bronze, Silver và quarantine |
| `test_iceberg_catalog.py` | Table schemas, partitions và approved evolution |
| `test_nyc_airflow_dag.py` | DAG topology, params, EMR và Cosmos config |
| `test_dbt_gold_contract.py` | Sáu models, grains, materializations, tests và Redshift target |
| `test_nyc_dbt_artifacts.py` | Successful, checksummed, reusable dbt evidence |
| `test_finalization_adapters.py` | Reconciliation, publication và verification |
| `test_publication_and_rerun.py` | Deterministic release, rerun và evolution evidence |
| `test_phase_c_deployment.py` | Terraform architecture, packaging và legacy absence |
| `test_redshift_data.py` | Data API result, failure và timeout behavior |

Fixtures nhỏ cố ý cover:

- Một valid row.
- Một exact duplicate.
- Một timeline error.
- Một unknown zone.
- Một negative amount.
- Field mới năm 2025.

Chúng chứng minh semantics, không chứng minh performance ở quy mô lớn.

### Local gate tối thiểu

Dùng virtualenv mới được tạo ở đúng repository path. Virtualenv là generated,
path-bound workstation state; phải tạo lại sau khi move/rename repo hoặc đổi
Python version.

```powershell
venv\Scripts\python.exe -m pytest -p no:cacheprovider tests/unit -q
venv\Scripts\python.exe scripts/package_spark_jobs.py `
  --output build/nyc_spark_jobs.zip --check
terraform -chdir=terraform fmt -check
terraform -chdir=terraform init -backend=false
terraform -chdir=terraform validate
```

Toàn bộ gate chi tiết nằm tại
[RUNBOOK.md](RUNBOOK.md#3-run-every-credential-independent-gate).

Local tests chứng minh pure logic, topology, packaging, SQL compilation và
Terraform structure. Chỉ retained AWS evidence mới chứng minh runtime thật.

## 26. Chương trình ownership bảy buổi

Mỗi buổi kết thúc bằng hai việc:

1. Bạn giải thích lại bằng lời của mình.
2. Bạn chạy một test hoặc chỉ ra một durable evidence kiểm chứng lời giải thích.

### Buổi 1 — Dựng lại kiến trúc

Đọc:

1. `README.md`.
2. `docs/ARCHITECTURE.md`.
3. Hai DAG definitions.

Bài tập: vẽ control/data/evidence planes và ghi tên dữ liệu đi qua mỗi mũi tên.

Đạt khi: giải thích được Glue chỉ là metadata, Gold do Redshift sở hữu và MWAA
là architecture boundary dù dataset nhỏ.

### Buổi 2 — Theo source identity vào Bronze

Đọc:

1. `etl/sources/nyc_hvfhs.py`.
2. `etl/contracts/nyc_hvfhs_identity.py`.
3. `etl/spark_jobs/nyc_bronze_ingestion.py`.
4. Source và identity tests.

Bài tập: lấy một fixture row, liệt kê input của `ingestion_run_id`, `row_id` và
`business_trip_key`. Thay đổi một field rồi dự đoán hash nào thay đổi.

Đạt khi: không nhầm source identity với row identity.

### Buổi 3 — Theo mọi Bronze row đến Silver hoặc quarantine

Đọc quality contract, Silver job và test reference helper cạnh nhau.

Bài tập: dự đoán reason/layer của từng fixture row trước khi chạy test.

Đạt khi: giải thích được reason priority, duplicate đứng cuối, `persist()` và
vị trí kiểm tra `Bronze = Silver + quarantine`.

### Buổi 4 — Theo Spectrum và dbt đến Gold

Đọc dbt sources, sáu models, schema tests, profile và Cosmos task group.

Bài tập: vẽ dbt graph, ghi grain/materialization của từng model. Giải thích khi
rerun tháng 1 thì model nào merge, model nào rebuild.

Đạt khi: phân biệt external Iceberg source với managed Redshift table và biết
chỉ `fct_trips` incremental.

### Buổi 5 — Hiểu release semantics

Đọc dbt artifact, reconciliation, publication builder/adapter và verification.

Bài tập: liệt kê toàn bộ evidence cần có trước publication. Giải thích identical
content và conflicting content được xử lý khác nhau ra sao.

Đạt khi: bảo vệ được việc tách reconciliation, publication và verification.

### Buổi 6 — Vận hành và recovery

Đọc `SEMANTICS.md` và các phần first month, rerun, backfill, schema evolution,
troubleshooting, teardown trong runbook.

Với mỗi failure stage, trả lời:

- Durable state nào có thể còn?
- Module/service nào là owner?
- Retry task nào?
- Hành động nào nguy hiểm?

Đạt khi: recovery không sửa source, xóa state, edit canonical data hoặc skip gate.

### Buổi 7 — Review một thay đổi

Chọn một thay đổi nhỏ như thêm mart metric hoặc quality rule. Trước khi code,
viết:

1. Contract hiện tại.
2. Contract mới.
3. Owners và consumers bị ảnh hưởng.
4. Historical/migration impact.
5. Test nhỏ nhất phải fail trước khi sửa.
6. Local và cloud evidence cần có.

Đạt khi: reviewer không phát hiện thêm layer bị bỏ sót.

## 27. Câu hỏi vấn đáp nghiệm thu

Bạn phải trả lời được từ code, không chỉ từ tài liệu này.

1. Vì sao hai Airflow runs có DAG run ID khác nhưng cùng `ingestion_run_id`?
2. Điều kiện nào chặn changed source ghi đè tháng đã được accept?
3. Vì sao `business_trip_key` không an toàn làm dbt merge key?
4. Vì sao Bronze vẫn chứa invalid và duplicate rows?
5. Nếu một duplicate row cũng sai timestamp thì reason nào thắng?
6. Retry một Silver month sẽ rewrite phần dữ liệu nào?
7. Vì sao cần snapshot ID khi row counts đã khớp?
8. Cosmos cung cấp gì ngoài việc chạy `dbt build`?
9. Gold relation nào incremental và merge key là gì?
10. Vì sao dbt thành công vẫn có thể không được publication?
11. Verification bổ sung thông tin gì sau reconciliation?
12. Backfill reuse monthly production path như thế nào?
13. State nào mutable và evidence nào immutable?
14. Cần thay đổi gì trước khi accept source 2025?
15. Những claim nào chưa được chứng minh trước khi chạy thật trên AWS?

Nếu câu trả lời chỉ dựa vào tài liệu, hãy mở file được chỉ ra và tìm đúng
condition, write, query hoặc test thực thi behavior đó.

## 28. Checklist trở thành owner

### Hiểu hệ thống

```text
[ ] Tôi vẽ được control, data và evidence planes.
[ ] Tôi trace được tám monthly Airflow stages theo đúng thứ tự.
[ ] Tôi biết canonical owner của từng table và state artifact.
[ ] Tôi phân biệt source identity, row identity và publication identity.
[ ] Tôi nói đúng grain và materialization của sáu Gold models.
```

### Hiểu best practices

```text
[ ] Tôi phân biệt retry, task clear, rerun, backfill và incremental.
[ ] Tôi giải thích được toàn bộ cơ chế phối hợp tạo idempotency.
[ ] Tôi hiểu deterministic quarantine và hai reconciliation equations.
[ ] Tôi hiểu explicit schema evolution và historical snapshot proof.
[ ] Tôi biết cái gì đã implement và cái gì chỉ là future extension.
```

### Vận hành

```text
[ ] Tôi chạy được local release gates và tìm đúng owner khi gate fail.
[ ] Tôi kiểm tra được Terraform state và reject destructive first plan.
[ ] Tôi trigger được một tháng và tìm đủ Airflow/EMR/manifest/dbt/publication evidence.
[ ] Tôi chứng minh được identical rerun mà không sửa landed source.
[ ] Tôi diagnose failure mà không bypass canonical owner.
[ ] Tôi hiểu bounded teardown và retained-data boundary.
```

### Thay đổi code

```text
[ ] Tôi lập change-impact map trước khi edit.
[ ] Tôi xác định historical compatibility và migration consequence.
[ ] Tôi thêm focused contract test và chạy neighboring suites.
[ ] Tôi cập nhật architecture/runbook khi boundary thay đổi.
[ ] Tôi biết cloud evidence nào cần có trước khi claim thành công.
```

## 29. Giới hạn cố ý của project

Owner phải biết hệ thống không claim điều gì:

- Upstream producer đã land immutable objects; source delivery ngoài repo.
- DAG chạy manual; chưa có automatic arrival event hoặc monthly schedule.
- `max_active_runs=1` và sequential backfill ưu tiên deterministic bounded run.
- Backfill đúng bốn tháng và không cross year.
- Dimensions/marts rebuild toàn bảng; chỉ fact incremental.
- Chưa có Iceberg compaction, snapshot expiration, orphan cleanup hoặc partition
  evolution automation.
- Chỉ có một explicit schema evolution, không có generic migration framework.
- Không có dashboard, Lake Formation, lineage platform, multi-account,
  streaming hoặc disaster recovery implementation.
- Small fixtures và bounded compute chứng minh architecture/correctness
  semantics, không chứng minh throughput TB/PB hoặc production concurrency.

Đây là scope boundaries, không phải danh sách feature cần tự động thêm khi
nghiệm thu.

## 30. Giải thích hệ thống trong một phút

Upstream producer land một immutable NYC monthly file và Taxi Zone reference
vào S3 với SHA-256 metadata. Airflow khóa requested month vào stable source
identity rồi tạo một EMR cluster tạm thời để chạy hai Spark steps. Bronze giữ nguyên source rows
và lineage trong Iceberg. Silver validate và exact-deduplicate, đồng thời đưa
mọi rejected row vào deterministic quarantine. Glue chia sẻ Iceberg metadata
cho Redshift Spectrum. Cosmos chạy và test dbt graph trong Redshift: merge
incremental trip fact và rebuild các dimensions/marts nhỏ. Redshift sau đó đối
chiếu Bronze, Silver, quarantine, Gold và operational manifest. Chỉ khi mọi
bằng chứng khớp, hệ thống mới ghi immutable publication JSON lên S3; cuối cùng
một consumer-style query đọc lại Silver và Gold để xác minh output đã publish.
Terraform sở hữu AWS resources và IAM boundaries. Local tests chứng minh code
contracts; retained cloud evidence mới hoàn tất nghiệm thu runtime thật.

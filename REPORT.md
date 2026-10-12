# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Nhật Thăng |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602727 |
| GitHub | nhat-thang |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-05297 |

> Cách tính trip: MSSV có chữ `A` nên lệnh `int(<MSSV>)` trong GUIDE không chạy được; em lấy phần số `202602727 % 26 = 15` → phần tử thứ 15 của `cli list-trips` là **trip-05297** (map_index 15 trong Airflow).

## Kết quả cuối

- `dataset_version` của bạn: `95e23eba8b87`  (khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: **743 / 129 / 147** (tổng 1019 khung; 18 / 4 / 4 trip)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (27/27 checks passed).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, `crash_log.png` (log task đỏ thấy trip-05297), `crash_rerun_green.png` (Graph xanh sau khi clear).

## Bước 1 · Bronze

### Câu 1
Khác biệt giữa A và B ở bronze (2 dòng đầu mỗi bảng do `step 1` in ra):
1. **Tên cột** khác: A `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer`; B `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, ay, throttle_pct, brake_pct, steering`.
2. **Đơn vị tốc độ**: A là m/s (`1.213`), B là km/h (`55.11`).
3. **Kiểu timestamp**: A là chuỗi ISO UTC (`2025-10-07T08:00:00.014Z`), B là epoch milli giây (`1759933801509`).
4. **Bàn đạp**: A ga/phanh dạng tỉ lệ 0–1 (`0.028`), B dạng phần trăm (`2.0`).
5. **Đồng hồ**: B lệch so với video, `telemetry_clock_offset_ms = 1500` (A là `0`) trong `bronze.video_files`.

Bronze giữ **mỗi nguồn một bảng** và **mọi cột là chữ** vì bronze là bản ghi nguyên gốc, chỉ thêm (append-only): nếu gộp và ép kiểu ngay, một lỗi parse (ô trống, `"abc"` trong cột số, đơn vị nhầm) sẽ làm mất hoặc làm sai dữ liệu mà không còn bản gốc để làm lại. Giữ chữ thô thì có thể sửa logic chuẩn hoá ở silver và chạy lại từ bronze bất cứ lúc nào. Mỗi bảng còn có `_source_file`, `_batch_id`, `_row_no`, `_ingested_at` để truy vết. Nạp lần hai báo `already present` (nhận diện bằng sha256), và `cli history bronze.raw_telemetry_a` chỉ có **một commit** (version 0, 3021 dòng) dù đã nạp hai lần.

## Bước 2 · Silver telemetry

### Câu 2
`stg_telemetry_b.sql` đổi tên cột của B về schema chung, đổi tốc độ **km/h → m/s** (chia 3,6), bàn đạp % → tỉ lệ, đổi **epoch ms → timestamp UTC**, rồi **trừ độ lệch đồng hồ 1500 ms** khai báo trong `sources.json`. Bằng chứng: sau chuẩn hoá, `bat_dau` của drive B là `2025-10-08 14:30:00.009+00` — trùng giờ bắt đầu video (14:30:00), check verify "drive B clock offset removed" PASS; tốc độ tối đa của hai drive là 71 và 100 km/h (hợp lý, dưới 130 km/h), tức đơn vị đã đúng.

Bảng quarantine của em:

| drive | duplicate | speed_missing | speed_out_of_range | tổng cách ly | sạch | nhận về |
|---|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 53 | 2968 | 3021 |
| drive_b | 20 | 30 | 3 | 53 | 3181 | 3234 |

`silver.telemetry` = 6149 dòng, `silver.telemetry_rejects` = 106 dòng, và sạch + cách ly = nhận về ở cả hai drive. Dòng lỗi vào `rejects` kèm `reject_reason` thay vì bị xoá vì: (1) có thể **đếm và giám sát** chất lượng nguồn (nếu cảm biến B đột nhiên có 500 dòng thiếu tốc độ, ta thấy ngay); (2) **kiểm toán được** — chứng minh không mất dòng nào; (3) nếu luật làm sạch sai (ví dụ ngưỡng 70 m/s quá chặt) thì có thể khôi phục các dòng đó mà không cần nạp lại.

### Câu 3
`max_gap_ms` (khoảng trống giữa 2 mẫu > 1000 ms) là `warn` vì mất tín hiệu cảm biến vài giây là **hiện tượng thật của dữ liệu**, không làm dữ liệu sai — khung hình ở đoạn đó chỉ bị đánh dấu `sync_lost` (7 khung mỗi drive) và loại khỏi gold; dừng cả pipeline vì nó thì không bao giờ chạy xong. Còn `in_range` (tốc độ 0–70 m/s) là `error` vì giá trị vô lý lọt vào silver nghĩa là **luật làm sạch đã hỏng**: dữ liệu sai sẽ đi tiếp vào gold và vào mô hình, nên phải dừng.

Thí nghiệm `break-gate` (`break_gate.json`): nới `max_speed_mps` từ 70 lên **1000** → `dbt test` cho `PASS=5 WARN=1 ERROR=1`; test **`in_range_silver_telemetry_speed_mps__70__0` bắt được 6 dòng** (đúng 3 + 3 dòng `speed_out_of_range` của hai drive đã lọt qua bước làm sạch). Khôi phục luật: `PASS=6 WARN=1 ERROR=0`. Điều này cho thấy cổng chất lượng là **lớp phòng thủ độc lập với code biến đổi**: dù ai đó sửa nhầm luật làm sạch, test vẫn kiểm tra hợp đồng của bảng đầu ra và chặn các task phía sau (trong Airflow, `dbt_test_silver` đỏ thì gold không chạy).

## Bước 3 · Silver frames

### Câu 4
`idempotency.json` (trip-00003, 60 khung):
- Trước: **1246 dòng**, dấu vân tay `6c7480dfde7e`; sau 2 lần chạy lại partition: **1246 dòng**, vân tay `6c7480dfde7e` → **không đổi** (`unchanged: true`).
- Ghi kiểu `append` chạy 2 lần cho cùng trip: **120 dòng** cho trip chỉ có 60 khung → **nhân đôi (2x)**.

`cli history silver.frames` cho thấy 2 commit mới (version 26, 27), mỗi commit `WRITE ... predicate: trip_id = 'trip-00003'`, `rows_added: 60` — tức mỗi lần là một **overwrite có predicate** chỉ thay đúng partition đó. Delta `overwrite` theo partition an toàn hơn "xoá rồi ghi lại" vì nó là **một giao dịch nguyên tử**: commit mới (xoá file cũ của trip + thêm file mới) chỉ xuất hiện trong `_delta_log` khi ghi xong toàn bộ. Nếu lỗi giữa chừng, commit không được tạo và người đọc vẫn thấy phiên bản cũ đầy đủ. Còn xoá-rồi-ghi là hai thao tác riêng: sập sau bước xoá thì trip **mất dữ liệu**, sập giữa lúc ghi thì còn **nửa trip**, và người đọc đồng thời có thể thấy trạng thái dở dang. Ngoài ra còn time travel để quay lại version trước.

### Câu 5
**Khung đen**: camera không có ảnh (độ sáng gần 0, gần như không có cạnh) → không có thông tin → **loại** (A: 6, B: 4). **Khung tối**: ảnh thật nhưng thiếu sáng (đường hầm, bóng râm), vẫn có đường, xe, vạch kẻ → **gắn cờ `is_dark` nhưng giữ lại** (A: 4, B: **61**). Em đồng ý: mô hình lái xe phải học được điều kiện thiếu sáng; bỏ khung tối sẽ làm dataset lệch về điều kiện đẹp. Cờ `is_dark` vẫn giúp lọc/cân bằng hoặc đánh giá riêng khi cần.

`threshold.json` (pipeline dùng ngưỡng 2):

| dHash khác tối đa | số khung trùng, drive A (/602) | số khung trùng, drive B (/644) |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Em chọn **ngưỡng 2**: ngưỡng 0 chỉ bắt khung giống hệt bit (bỏ sót đoạn camera đứng hình có nhiễu nén nhỏ), còn ngưỡng 6 loại 60% khung của A và 38% của B — nhiều khung trong đó là cảnh khác nhau thật khi xe chạy chậm. Ở ngưỡng 2, verify bắt được 11/12 khung đứng hình ở A. Ngưỡng phụ thuộc **tốc độ xe**: A chạy chậm (val chỉ 19 km/h) nên hai khung cách nhau 0,5 s gần như giống nhau → nhiều khung trùng hơn hẳn B (149 so với 53 ở ngưỡng 2), dù B dài hơn. Hợp lý hơn là ngưỡng thích ứng theo tốc độ (xe đứng yên thì nới, xe nhanh thì giữ chặt) hoặc lấy mẫu theo quãng đường thay vì theo thời gian.

## Bước 4 · Gold

### Câu 6
`leakage.json`: 1019 khung đáng dùng, thuộc 26 trip.
- Chia **theo khung hình** (ngẫu nhiên 70/15/15): **26/26 trip** nằm ở nhiều hơn một split, và **26 trip có khung ở cả train lẫn test**.
- Chia **theo trip** (gold của pipeline): **0 trip** bị chia cắt (verify "no trip is in two splits" PASS).

Lấy mẫu 2 khung/giây nên hai khung liền nhau trong một trip gần như giống hệt (cùng đường, cùng xe phía trước, cùng ánh sáng). Nếu chia theo khung, ảnh ở test có "anh em sinh đôi" ở train → mô hình chỉ cần **nhớ** cảnh là đoán đúng, điểm test cao nhưng không đo khả năng **tổng quát hoá** sang cảnh/đường chưa thấy. Khi ra đường thật, hiệu năng sẽ tụt. Chia theo trip đảm bảo test là cảnh mới hoàn toàn.

### Câu 7
`dataset_version` = 12 ký tự đầu của sha256 trên danh sách `frame_id|split` đã sắp xếp (`pipeline/publish.py`). Vì là **băm nội dung**, cùng nội dung → cùng version, khác một khung hay một split → version khác. Ngày giờ hay số đếm thì chạy lại cùng dữ liệu cũng ra version mới (không nhận ra hai dataset giống nhau), và hai dataset khác nhau có thể trùng số đếm ở hai máy. Kết quả của em: `step all` lúc đầu, `step 4`, `cli export-dataset`, và DAG Airflow đều cho **`95e23eba8b87`** — ổn định qua các lần chạy.

Version khác bạn bè vì trong `gold_dataset_frames.sql`, thứ tự trip được xếp theo `md5(trip_id || LAB_STUDENT_ID)`: MSSV đổi thì thứ tự đổi, trip rơi vào split khác, danh sách `frame_id|split` đổi, nên hash đổi. Em thử chạy `step 4` với MSSV giả `2A202600000`: version thành **`28d927dbdedd`**, train/val/test = 729 / 160 / 130 (của em là 743 / 129 / 147), tổng vẫn 1019 khung, vẫn 18/4/4 trip. Chạy lại với MSSV của em thì về đúng `95e23eba8b87`.

### Câu 8
`lineage` trong `lake/gold/datasets/95e23eba8b87/manifest.json`:

```json
"lineage": {
  "bronze.raw_telemetry_a": 0, "bronze.raw_telemetry_b": 0, "bronze.trips_raw": 0, "bronze.video_files": 1,
  "silver.frames": 27, "silver.telemetry": 0, "silver.frames_flagged": 0, "silver.frame_telemetry": 0,
  "input_video_sha256": {
    "drive_a": "636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa",
    "drive_b": "bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc"
  }
}
```

Dataset này được tạo từ hai video có sha256 trên, telemetry thô A/B ở version 0, bảng trips version 0, bảng `silver.frames` ở **version 27**, và các bảng silver dbt ở version 0. `silver.frames` ở version 27 vì có 26 lần ghi partition (version 0–25) cộng 2 lần chạy lại trip-00003 trong thí nghiệm idempotent. Ở lần chạy cuối `step all --reset` từ lake trống, manifest ghi `"silver.frames": 25` (đúng 26 commit, mỗi trip một commit), mọi giá trị khác giữ nguyên, và **`dataset_version` vẫn là `95e23eba8b87`**. Như vậy version của bảng trung gian ghi lại *lịch sử ghi*, còn `dataset_version` chỉ phụ thuộc *nội dung*. Manifest còn có `rules` (loại `is_black, is_blurry, is_near_dup, not sync_ok`; split theo trip, phân tầng theo drive, thứ tự md5) và `manifest_sha256`. Ba tháng sau, để tái tạo: dùng **time travel** của Delta đọc đúng các version bảng trên, đối chiếu sha256 video gốc, cộng thêm **commit git của code/dbt** và **MSSV (salt split)**, rồi kiểm tra lại hash ra đúng `95e23eba8b87`. Thiếu sót: manifest chưa ghi commit git của code và tham số (`sync_tol_ms`, ngưỡng dHash), nên em sẽ bổ sung.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Đặt `LAB_FAIL_ONCE=trip-05297`, trigger `day09_reset` rồi `day09_video_pipeline` (run `pipeline_crash`). Kết quả trên 37 task instance:
- **1 task đỏ**: `silver_frames` map_index **15**; log có dòng `RuntimeError: simulated crash while extracting trip-05297; run it again`.
- **7 task `upstream_failed`**: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `export_dataset`, `publish_lake`, `publish_warehouse`.
- **29 task success**: 3 task bronze, `trip_commands` và 25 trip `silver_frames` còn lại — chúng **không chạy lại**.

Em **chỉ clear task đỏ** `silver_frames[15]` (kèm các task phía sau để chúng chạy tiếp). Đúng **8 task chạy lại**: `silver_frames[15]` rồi 7 task phía sau. 25 trip khác và 3 task bronze giữ nguyên kết quả cũ. Run chuyển xanh **37/37 success** sau khoảng 1 phút. Sau đó `scripts/lab.sh verify` cho **27/27**. `dataset_version` vẫn là **`95e23eba8b87`** (giống run xanh trước đó và `run_summary.json`). `lake.silver_frames` có **1246 dòng / 1246 `frame_id` khác nhau**, trip-05297 có 31 dòng. Delta history của `silver.frames` chỉ có **một** commit `WRITE predicate: trip_id = 'trip-05297'` (`rows_added: 31`), từ lần chạy lại.

Không nhân đôi vì `silver_frames` ghi bằng Delta overwrite theo predicate `trip_id` (giống Câu 4): lần sập xảy ra **trước khi commit**, nên Delta không có gì của trip-05297 ở lần đó, và lần chạy lại ghi đúng một partition. Bằng chứng: ảnh `report/screenshots/crash_log.png` (log task đỏ có trip-05297) và `crash_rerun_green.png` (Graph xanh sau khi clear).

### Câu 10
Với 500 xe mỗi ngày (so với 2 drive × khoảng 5 phút, 1246 khung), ba chỗ vỡ đầu tiên:
1. **Giải mã video / trích khung** (slide 24): hiện tại FFmpeg chạy tuần tự trong **một container Airflow `LocalExecutor`, `PARALLELISM=2`, `max_active_tis_per_dagrun=1`**. 26 trip mất khoảng 1 phút; 500 xe × hàng giờ video × 7 camera sẽ cần hàng nghìn CPU-giờ. Thay bằng cụm xử lý phân tán (Spark/Ray trên Kubernetes, giải mã bằng GPU NVDEC), Airflow chỉ điều phối (KubernetesExecutor/CeleryExecutor), worker tự co giãn.
2. **Điều phối một task cho mỗi trip** (slide 12, 23): dynamic task mapping 26 task là ổn, nhưng hàng chục nghìn trip/ngày sẽ làm nghẽn scheduler và metadata DB. Chuyển sang lô theo xe/giờ (mỗi task xử lý nhiều trip), ingest theo sự kiện (xe upload → Kafka/S3 event → hàng đợi), backfill theo partition ngày.
3. **DuckDB + dbt trên một node và MinIO một node** (slide 22): DuckDB đọc toàn bộ bảng trong RAM một máy; bảng frames/telemetry hàng tỉ dòng sẽ vượt bộ nhớ. Thay bằng engine phân tán (Spark/Trino/Databricks SQL với dbt adapter), Delta phân vùng theo `date/vehicle` kèm OPTIMIZE/Z-ORDER để gom file nhỏ. Lưu trữ chuyển sang S3 thật nhiều AZ; ảnh JPEG lẻ gom thành shard (WebDataset/tar, Parquet) cho dataloader. Postgres warehouse cũng nên chuyển sang warehouse cột (ClickHouse/BigQuery) cho BI.

## Câu hỏi riêng của bạn trên Metabase

Câu hỏi: **"Trip nào có tỷ lệ khung bị loại cao nhất, và có liên quan tới tốc độ không?"**

```sql
select trip_id, drive_id, split,
       round(avg_speed_kmh::numeric, 1)                          as toc_do_tb_kmh,
       frames, frames_kept,
       round(100.0 * (frames - frames_kept) / frames, 1)        as pct_bi_loai
from marts.mart_trip_summary
order by pct_bi_loai desc
```

Kết quả (top 5): trip-04622 (A, 67 km/h) 56,7%; trip-03904 (A, 46 km/h) 50,0%; trip-02086 (A, 17,6 km/h) 46,7%; trip-08888 (A, 6,5 km/h) 40,0%; trip-07774 (A, 12,8 km/h) 38,2%. Toàn bộ 26 trip: 1246 khung, giữ 1019, **loại 18,2%**. Diễn giải: các trip bị loại nhiều nhất đều ở **drive A**, và gần như toàn bộ do cờ **khung trùng** (tra `silver_frames_flagged`: trip-04622 có 34 khung trùng, 0 đen, 0 mờ; trip-02086 có 28; trip-08888 có 24). Với trip chạy chậm (6–18 km/h) thì hợp lý: xe gần đứng yên nên khung cách 0,5 s giống nhau. **trip-03904** (50%) là trip chứa lỗi cấy "camera đứng hình 6 giây" (giây 150–156 của drive A = 08:02:30–08:02:36, nằm trong trip này): 11/22 khung bị đánh dấu trùng, khớp với 12 khung đứng hình. Đáng ngờ nhất là **trip-04622**: chạy **67 km/h**, không có lỗi cấy, không mất đồng bộ, mà vẫn loại 56,7% vì "trùng". Em đoán đây là đoạn đường cao tốc ít chi tiết (trời, mặt đường đồng màu), nên dHash 64 bit của ảnh thu nhỏ chỉ lệch ≤ 2 bit dù cảnh đã thay đổi → **dương tính giả** của luật khung trùng. Vận hành nên xem ảnh của trip này trước khi tin con số, và cân nhắc ngưỡng dHash phụ thuộc tốc độ (xem Câu 5). Vì trip-04622 và trip-02086 ở **train**, train của em mất khoảng 60 khung drive A.

**Câu hỏi thứ hai (điểm cộng): split test có phủ đủ dải tốc độ như train không?**

```sql
select split,
       case when speed_kmh < 10 then '0-10' when speed_kmh < 30 then '10-30'
            when speed_kmh < 50 then '30-50' else '50+' end              as dai_toc_do,
       count(*)                                                          as khung,
       round(100.0 * count(*) / sum(count(*)) over (partition by split), 1) as pct_trong_split
from marts.mart_frame_gallery
group by 1, 2 order by 1, 2
```

| split | 0–10 km/h | 10–30 | 30–50 | 50+ |
|---|---|---|---|---|
| train (743) | 4,3% | 11,2% | 24,6% | 59,9% |
| val (129) | 14,7% | 13,2% | 7,0% | 65,1% |
| test (147) | **0,7%** (1 khung) | 17,7% | **0,7%** (1 khung) | **81,0%** |

Với split của MSSV em, test gần như **không có khung ở dải 0–10 và 30–50 km/h** (mỗi dải 1 khung), còn 81% là chạy nhanh. Vì chỉ 4 trip test và chia theo trip, một mô hình đánh giá trên test này sẽ **không được kiểm tra** ở tình huống chạy chậm/đô thị. Đây là cái giá của chia theo trip với ít trip: không rò rỉ nhưng phân phối lệch. Cách sửa: phân tầng split theo cả dải tốc độ/điều kiện (không chỉ theo drive), hoặc tăng số trip.

## Nhìn lại

- Bất ngờ nhất: chia theo khung làm **26/26 trip** bị rò rỉ sang test — không phải "một ít" mà là toàn bộ. Và thứ tự split chỉ phụ thuộc một chuỗi salt (MSSV) mà version đổi hoàn toàn.
- Gặp sự cố thật: chạy `scripts/lab.sh` trên Git Bash (Windows) báo `Cwd must be an absolute path` vì MSYS tự đổi `/opt/lab` thành đường dẫn Windows; phải đặt `MSYS_NO_PATHCONV=1`. Lệnh tính trip trong GUIDE dùng `int(MSSV)` không chạy với MSSV có chữ.
- Chưa rõ: verify chỉ bắt **11/12** khung đứng hình ở ngưỡng 2 — khung còn lại có lẽ do nhiễu nén làm dHash lệch hơn 2 bit; em chưa kiểm tra cụ thể.
- Lỗi dữ liệu pipeline **chưa** bắt: (1) telemetry có giá trị *hợp lệ về miền nhưng vô lý về vật lý*, ví dụ tốc độ nhảy 0 → 60 m/s trong 0,1 s (gia tốc không thể có) vẫn qua `in_range`; (2) độ lệch đồng hồ chỉ trừ theo **giá trị khai báo** trong `sources.json` — nếu khai báo sai hoặc đồng hồ trôi dần theo thời gian thì không có test nào đối chiếu (có thể kiểm tra tương quan giữa chuyển động trong video và tốc độ); (3) GPS nhảy vị trí không bị kiểm tra.

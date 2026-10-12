# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Vũ Quốc Huy` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602929` |
| GitHub | `VuQuocHuy89` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-02736` |

## Kết quả cuối

- `dataset_version` của bạn: `0365e9052597`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `711 / 150 / 158`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

A dùng `ts_utc` dạng ISO UTC, B dùng `timestamp_ms` dạng epoch mili giây và có lệch đồng hồ khai báo 1.500 ms.
A dùng `speed_mps`, `heading_deg`, `lat`, `lon`, `gas`, `brake`, `steer`; B tương ứng dùng `spd_kmh`, `hdg`, `latitude`, `longitude`, `throttle_pct`, `brake_pct`, `steering`. Tốc độ và bàn đạp còn khác đơn vị.
Bronze giữ riêng 3.021 dòng A và 3.234 dòng B dưới dạng chữ để bảo toàn dữ liệu nguồn, kể cả ô trống hoặc giá trị lỗi, trước khi Silver chuẩn hóa và phân loại.
Nạp lại A báo `already present`; lịch sử `bronze.raw_telemetry_a` vẫn chỉ có một commit với 3.021 dòng, cho thấy lần nạp lặp không nhân đôi dữ liệu.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Staging đổi tên cột B sang schema của A, chia `spd_kmh` cho 3,6 để ra m/s và chia `throttle_pct`, `brake_pct` cho 100. Timestamp epoch mili giây của B được trừ offset 1.500 ms rồi đổi sang UTC; A vốn dùng ISO UTC và m/s.
Sau chuẩn hóa, thời điểm đầu của B là `2025-10-08 14:30:00.009+00:00`, lệch 9 ms so với đầu video; tốc độ tối đa của A/B lần lượt là 71/100 km/h. Có 2.968 dòng sạch A và 3.181 dòng sạch B.
Quarantine: drive A — `duplicate` 20, `speed_missing` 30, `speed_out_of_range` 3; drive B cũng lần lượt 20, 30, 3. Mỗi drive có 53 dòng rejects, nên 2.968 + 53 = 3.021 và 3.181 + 53 = 3.234 dòng đã nhận.
Giữ dòng lỗi và lý do trong `silver.telemetry_rejects` giúp kiểm tra nguồn và đối soát tổng số dòng; xóa chúng sẽ che mất lỗi dữ liệu.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` báo khoảng mất tín hiệu cảm biến để điều tra nhưng vẫn cho pipeline chạy; bước 2 có 6 PASS, 1 WARN và 0 ERROR. `in_range` chặn tốc độ ngoài 0–70 m/s vì giá trị vô lý không nên đi tiếp.
Trong `break_gate.json`, khi nới luật làm sạch lên 1.000 m/s, test `in_range_silver_telemetry_speed_mps__70__0` phát hiện 6 dòng: 5 PASS, 1 WARN, 1 ERROR. Sau khi khôi phục luật gốc, kết quả trở lại 6 PASS, 1 WARN, 0 ERROR.
Vì vậy cổng chất lượng vẫn phát hiện và chặn dữ liệu xấu lọt qua bước làm sạch; cảnh báo về khoảng trống cảm biến không tự động dừng toàn pipeline.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Thí nghiệm trên `trip-00003` có 1.246 dòng Silver và dấu vân tay `6c7480dfde7e` trước lẫn sau hai lần chạy lại. Riêng trip này có 60 khung; mô phỏng ghi `append` hai lần tạo 120 dòng, tức gấp đôi.
Delta `overwrite` theo điều kiện `trip_id` thay toàn bộ partition bằng một commit và giữ phiên bản trước để phục hồi. Nếu xóa rồi ghi riêng, lỗi giữa hai thao tác có thể để lại partition trống hoặc ghi dở; chạy lại bằng `append` có thể nhân đôi dữ liệu.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung đen là lỗi mất hình nên bị loại khỏi Gold (A: 6, B: 4). Khung tối vẫn chứa cảnh có ích nên chỉ gắn cờ và giữ lại (A: 4, B: 61); tôi đồng ý vì loại mọi cảnh tối sẽ làm mất dữ liệu đường hầm hoặc bóng râm.
Tôi chọn ngưỡng dHash 2 đang dùng: nó bắt nhiều khung gần trùng hơn ngưỡng 0 nhưng không loại quá rộng như ngưỡng 6. Khi xe chạy chậm, cảnh thay đổi ít nên dHash giữa hai khung dễ nhỏ; ngưỡng quá cao có thể loại cả biến đổi thật, còn xe chạy nhanh thường tạo khác biệt lớn hơn giữa các khung.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Trong 1.019 khung hình dùng được thuộc 26 trip, cách chia ngẫu nhiên theo khung làm cả 26 trip xuất hiện ở nhiều split; cả 26 trip đều có khung ở train và test. Cách chia theo trip của Gold có 0 trip bị chia cắt.
Các khung gần nhau trong một trip có cùng cảnh và tương quan mạnh. Nếu train đã chứa những khung sát với test, mô hình có thể nhận lại cảnh quen thuộc; điểm test khi đó đánh giá quá lạc quan khả năng làm việc trên trip mới.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` được tính từ danh sách `frame_id|split`, nên cùng nội dung và cách chia sẽ cho cùng mã bất kể thời điểm chạy. Sau bước 4 và lần `export-dataset` kế tiếp, tôi đều nhận `0365e9052597`, gồm 711 train, 150 val, 158 test.
MSSV `2A202602929` được đưa vào thứ tự băm khi xếp trip vào split; MSSV khác có thể đổi split của frame, kéo theo đổi chuỗi đầu vào của băm và `dataset_version`. Mã này giúp phát hiện dataset thay đổi và đối chiếu lần chạy lại.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`manifest.json` của `0365e9052597` ghi nguồn Bronze: `raw_telemetry_a` v0, `raw_telemetry_b` v0, `trips_raw` v0, `video_files` v1; nguồn Silver: `frames` v25, `telemetry` v0, `frames_flagged` v0, `frame_telemetry` v0.
Hai video đầu vào có SHA256 lần lượt là A `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa` và B `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. Manifest còn ghi 1.019 frame và quy tắc loại khung đen, mờ, gần trùng, mất đồng bộ; split theo trip.
Để tái tạo, cần đúng video theo SHA256, các phiên bản bảng nguồn, quy tắc xử lý/cấu hình và MSSV `2A202602929` dùng để chia split; sau đó đối chiếu `dataset_version` và manifest của lần chạy mới.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Do MSSV có chữ A, tôi lấy các chữ số `2202602929`; `2202602929 % 26 = 7`, tương ứng `trip-02736`. Khi đặt `LAB_FAIL_ONCE=trip-02736`, đúng một task `silver_frames[7]` đỏ; log ghi `simulated crash while extracting trip-02736`, còn 7 task phía sau thành `upstream_failed`.
Tôi chỉ chọn clear `silver_frames[7]`; Airflow đưa 7 task phụ thuộc vào danh sách, tổng cộng 8 task được chạy lại. Sau đó DAG xanh, `verify` đạt 27/27, Gold có 1.019 khung và `dataset_version` vẫn là `0365e9052597`, không có dấu hiệu nhân đôi.
Ảnh `fail_task_log.png` cho thấy trip và lỗi; `airflow_retry_graph.png` cho thấy Graph xanh sau khi clear.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Tôi dự kiến khâu FFmpeg/trích frame trên một máy hết CPU trước: 2 drive đã tạo 1.246 frame từ 26 trip; 500 xe/ngày làm lượng giải mã tăng rất mạnh. Tôi sẽ phân phối các trip tới nhiều worker, giữ đầu vào/ảnh trên object storage.
Airflow hiện dùng `LocalExecutor` trong một container; khoảng 6.500 task trip/ngày sẽ gây áp lực lên scheduler và máy chạy task. Tôi sẽ dùng executor có worker mở rộng theo tải và giới hạn đồng thời cho từng tài nguyên.
DuckDB hiện ghi vào một file `work/lakehouse.duckdb`; dữ liệu và phép join/kiểm tra tăng sẽ vượt khả năng một máy. Tôi sẽ chuyển phần biến đổi lớn sang engine phân tán hoặc warehouse mở rộng được, vẫn giữ quality gate trước Gold.

## Câu hỏi riêng của bạn trên Metabase

Các câu SQL bạn thêm vào dashboard và điều chúng cho biết:

Câu hỏi: trip nào giữ lại tỷ lệ frame thấp nhất sau kiểm tra chất lượng?
`select drive_id, trip_id, frames, frames_kept, round(100.0 * frames_kept / nullif(frames, 0), 1) as kept_pct from marts.mart_trip_summary order by kept_pct, trip_id limit 5`
Kết quả thấp nhất là `drive_a / trip-04622`: 26/60 khung được giữ, tức 43,3%; tiếp theo là `trip-03904`: 11/22, tức 50,0%. Các trip này nên được xem lại cờ chất lượng trước khi dùng để huấn luyện.

Câu hỏi thứ hai (bonus): với từng drive và split, nhóm nào có tỷ lệ giữ frame thấp nhất để ưu tiên rà soát?
`select drive_id, split, count(distinct trip_id) as trips, sum(frames) as frames, sum(frames_kept) as frames_kept, round(100.0 * sum(frames_kept) / nullif(sum(frames), 0), 1) as retention_pct from marts.mart_trip_summary group by drive_id, split order by drive_id, split`
Kết quả thấp nhất là `drive_a / train`: giữ 280/418 frame (67,0%), so với `drive_b / train` giữ 431/486 (88,7%). Dashboard giúp ưu tiên rà soát nhóm drive_a/train.

## Bonus · Thí nghiệm riêng

Tôi tăng `sync_tol_ms` từ 150 ms lên 1.000 ms và chạy lại model `silver_frame_telemetry`. Ở 150 ms, drive A đồng bộ 595/602 frame, B 637/644; tổng cộng 14/1.246 frame mất đồng bộ.
Ở 1.000 ms, A đồng bộ 597/602, B 638/644; số frame mất đồng bộ giảm từ 14 xuống 11, tức giảm 3 frame (21,4%). Ngưỡng rộng hơn có thể ghép frame với telemetry cũ đến một giây, nên tôi khôi phục 150 ms.
Sau khi khôi phục, `step all --reset` và kiểm tra toàn bộ đạt 27/27; `dataset_version` vẫn là `0365e9052597`.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Tôi bất ngờ vì B có 61 khung tối vẫn được giữ, trong khi A có 149 khung gần trùng bị loại; hai cờ chất lượng không có cùng ý nghĩa.
Tôi còn muốn kiểm tra ngưỡng dHash 2 trên nhiều tốc độ và điều kiện ánh sáng hơn trước khi dùng cho dữ liệu mới.
Một lỗi có thể chưa được bắt là tọa độ GPS nhảy đột ngột nhưng từng giá trị vẫn nằm trong khoảng hợp lệ; cần thêm kiểm tra tốc độ di chuyển suy ra từ các điểm GPS liên tiếp.

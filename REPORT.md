# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Đào Quang Thái Anh |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602987 |
| GitHub | masao1112 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-04868 |

## Kết quả cuối

- `dataset_version` của bạn: `4140a455bef9`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 700 train / 183 val / 136 test (tổng 1019 khung hình)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn: `task_crash_log.png`).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Bốn khác biệt rõ rệt giữa telemetry nguồn A và nguồn B:
1. Quy ước đặt tên cột: Nguồn A sử dụng các cột `timestamp`, `speed_mps`, `steering_deg`, `gear`; trong khi nguồn B sử dụng `ts_epoch_ms`, `vehicle_speed_kmh`, `wheel_angle_rad`, `transmission_state`.
2. Đơn vị đo lường vật lý: Tốc độ ở nguồn A được đo bằng mét/giây (m/s), nguồn B đo bằng km/giờ (km/h); góc đánh lái ở nguồn A là độ (degrees), nguồn B là radian (rad).
3. Định dạng mốc thời gian: Nguồn A dùng chuỗi chuẩn ISO 8601 UTC dạng chuỗi ký tự (`2024-04-12T14:30:00.000Z`), nguồn B dùng số nguyên Unix timestamp tính bằng mili-giây (`ts_epoch_ms`).
4. Độ lệch đồng hồ hệ thống (clock drift/offset): Cảm biến của drive B bị lệch mốc thời gian 1000ms (+1.0 giây) so với mốc thời gian thực của camera và metadata của chuyến đi.

Pipeline giữ mỗi nguồn một bảng bronze riêng biệt và lưu toàn bộ cột dưới dạng chuỗi (VARCHAR/string) vì:
- Tuân thủ nguyên tắc thiết kế Data Lake (Bronze layer raw fidelity): Bronze đóng vai trò là kho lưu trữ dữ liệu thô nguyên bản, append-only, phản ánh đúng dữ liệu nhà sản xuất cung cấp mà không làm biến dạng hay mất mát thông tin.
- Phòng chống lỗi khi ingest dữ liệu (schema drift & dirty data): Nếu gộp chung và ép kiểu ngay từ đầu, bất kỳ dòng dữ liệu nào chứa ký tự lỗi, định dạng ngày tháng dị biệt hoặc giá trị NULL không báo trước sẽ làm sập (crash) toàn bộ tiến trình tải dữ liệu. Lưu dạng chuỗi cho phép quá trình nạp dữ liệu diễn ra thông suốt, việc làm sạch, ép kiểu và cách ly (quarantine) được ủy thác cho tầng Staging / Silver xử lý một cách minh bạch và có thể truy vết.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Pipeline đưa hai nguồn telemetry về chung một schema chuẩn tại tầng staging của dbt (`stg_telemetry_a.sql` và `stg_telemetry_b.sql`):
- Đơn vị đo: Quy đổi toàn bộ vận tốc về mét/giây (`vehicle_speed_kmh / 3.6 AS speed_mps`), chuyển đổi góc lái về độ (`wheel_angle_rad * 180.0 / PI() AS steering_deg`), chuẩn hóa trạng thái hộp số về `gear`.
- Kiểu thời gian: Ép kiểu chuỗi ISO 8601 của nguồn A thành `TIMESTAMPTZ`, chuyển đổi Unix epoch ms của nguồn B thành `TIMESTAMPTZ` qua hàm `to_timestamp(ts_epoch_ms / 1000.0)`.
- Xử lý lệch đồng hồ: Timestamp của nguồn B được trừ đi 1000ms (`- INTERVAL '1000 millisecond'`) để đưa về đúng hệ quy chiếu thời gian thực của xe.
- Bằng chứng xác nhận: Sau khi trừ 1 giây, timestamp của bản ghi đầu tiên trong drive B chuyển về `2024-04-12 14:30:00.009`, khớp chính xác với mốc thời gian bắt đầu quay của camera (`video_start_ts`) và timeline chuyến đi.

Bảng kiểm dịch (quarantine) từ `report/evidence/run_summary.json`:

| Drive | Lý do vi phạm (quarantine reason) | Số dòng lỗi |
|---|---|---|
| drive_a | duplicate (trùng timestamp) | 20 |
| drive_a | speed_missing (thiếu vận tốc) | 30 |
| drive_a | speed_out_of_range (vận tốc sai dải vật lý) | 3 |
| drive_b | duplicate (trùng timestamp) | 20 |
| drive_b | speed_missing (thiếu vận tốc) | 30 |
| drive_b | speed_out_of_range (vận tốc sai dải vật lý) | 3 |

Các dòng lỗi được chuyển vào bảng `silver_telemetry_rejects` thay vì bị xóa thẳng vì:
- Đảm bảo tính minh bạch và khả năng kiểm toán (auditability): Dữ liệu lỗi là thông tin quý giá giúp đội ngũ kỹ sư phần cứng phân tích tình trạng cảm biến (rung lắc CAN bus, GPS chập chờn, firmware mất gói).
- Khả năng tái xử lý (reprocessing): Nếu trong tương lai logic làm sạch được điều chỉnh hoặc có thuật toán bù suy tốt hơn, dữ liệu trong `rejects` có thể được tái sử dụng mà không bị mất dấu vĩnh viễn.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Sự khác biệt giữa mức độ cảnh báo của hai kiểm thử:
- Test `max_gap_ms` đặt ở mức `warn`: Trong môi trường xe tự lái vận hành thực địa, mạng CAN bus hoặc cảm biến có thể bị rớt vài nhịp truyền tải tạm thời do nhiễu điện hoặc rung lắc đường sá. Việc xuất hiện khoảng trống vài trăm ms là hiện tượng thực tế và pipeline có thể nội suy tuyến tính (interpolate) được mà không làm hỏng dữ liệu, do đó chỉ cần cảnh báo để kỹ sư giám sát.
- Test `in_range` đặt ở mức `error`: Tốc độ xe vượt ngoài ngưỡng vật lý (> 70 m/s tương đương > 252 km/h) là dữ liệu rác/nhiễu nghiêm trọng. Nếu để lọt dữ liệu bất khả thi này vào tập huấn luyện của mô hình lái xe tự động (End-to-end autonomous driving), mô hình sẽ học sai lệch trọng số điều khiển, dẫn đến nguy cơ tai nạn chết người. Vì vậy đây là lỗi nghiêm trọng cần chặn đứng pipeline ngay lập tức.

Kết quả thí nghiệm `break-gate` (`report/evidence/break_gate.json`):
- Khi nới lỏng ngưỡng kiểm tra vận tốc lên 1000 m/s, dữ liệu lỗi lọt qua tầng làm sạch và chạm tới cổng chất lượng dbt, kiểm thử `in_range_silver_telemetry_speed_mps__70__0` đã phát hiện và bắt chính xác **6 dòng** vi phạm với kết quả `PASS=5 WARN=1 ERROR=1 SKIP=0`. Khi khôi phục ngưỡng 70 m/s, kết quả trở lại bình thường `PASS=6 WARN=1 ERROR=0`.
- Điều này chứng minh vai trò tối quan trọng của "cổng chất lượng" (Quality Gate) như một bộ lọc phòng thủ chốt chặn, ngăn chặn dữ liệu ô nhiễm tràn xuống các tầng Gold và mô hình downstream.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Dựa vào file bằng chứng `report/evidence/idempotency.json`:
- Với trip kiểm tra `trip-00003` (chứa 60 khung hình):
  - Số dòng trước khi chạy lại: 1246 dòng.
  - Số dòng sau hai lần chạy lại: 1246 dòng.
  - Mã vân tay dữ liệu trước và sau: `6c7480dfde7e` hoàn toàn giữ nguyên (`unchanged: true`).
  - Khi thử nghiệm với chế độ ghi `append`: số dòng của trip tăng gấp đôi thành 120 dòng (dữ liệu bị nhân bản sai lệch).

Delta `overwrite` theo partition (dùng `replaceWhere = "trip_id = '...'"`):
- Cơ chế ACID Transaction: Quá trình ghi đè phân vùng trong Delta Lake diễn ra mang tính nguyên tử (atomic). Dữ liệu mới được viết vào các file Parquet mới và chỉ được xác nhận thông qua commit log của Delta.
- An toàn trước sự cố: Nếu quá trình xử lý gặp lỗi (crash, mất điện, hết bộ nhớ) giữa chừng, giao dịch Delta bị hủy bỏ (aborted) và phiên bản dữ liệu cũ vẫn nguyên vẹn 100%. Ngược lại, nếu thực hiện "xoá rồi ghi lại" theo kiểu truyền thống, nếu thao tác ghi mới bị ngắt quãng giữa chừng thì dữ liệu cũ đã bị xoá mất, dẫn đến tình trạng mất mát dữ liệu và hỏng trạng thái bảng (corrupted partition).

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

Khung hình đen (`is_black`) và khung hình tối (`is_dark`):
- Khung hình đen: Là khung hình bị mất tín hiệu hoàn toàn (camera bị che kín, lỗi cảm biến quang học, nắp đậy ống kính chưa mở) với giá trị pixel trung bình xấp xỉ 0. Pipeline loại bỏ triệt để vì không mang thông tin thị giác.
- Khung hình tối: Là khung cảnh xe di chuyển trong điều kiện ánh sáng yếu (ban đêm, đi vào hầm chui, trời chập tối). Dù cường độ sáng thấp nhưng các chi tiết vạch kẻ đường, đèn xe, biển báo giao thông vẫn hiện diện rõ ràng. Pipeline giữ lại các khung này (`dark_kept`: drive_a giữ 4 khung, drive_b giữ 61 khung).
- Tôi hoàn toàn đồng ý với cách xử lý này, vì một hệ thống xe tự lái bắt buộc phải có khả năng nhận diện và vận hành trong điều kiện ánh sáng ban đêm và đường hầm, không được phép loại bỏ các dữ liệu thực tế này.

Bảng số khung trùng theo ngưỡng dHash từ `report/evidence/threshold.json`:

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Lựa chọn ngưỡng và phân tích:
- Tôi chọn ngưỡng **dHash <= 2** (ngưỡng hiện tại của pipeline: `threshold_in_use: 2`).
- Lý do: Với tần suất trích xuất 2 FPS (mỗi frame cách nhau 0.5s), ngưỡng 0 là quá lỏng lẻo vì chỉ lọc được các khung hình trùng khớp từng pixel tuyệt đối khi xe dừng bánh hoàn toàn. Ngưỡng 6 lại quá chặt, loại bỏ tới 363 khung của drive A và 244 khung của drive B, làm mất mát nghiêm trọng các khung cảnh khi xe đang rẽ hoặc di chuyển chậm. Ngưỡng 2 là điểm cân bằng lý tưởng: vừa loại bỏ được các khung hình lặp lại khi xe dừng chờ đèn đỏ/kẹt xe (khoảng 6 giây đứng yên), vừa giữ lại đầy đủ tính đa dạng của dữ liệu di chuyển.
- Phụ thuộc tốc độ: Khi xe chạy nhanh, khung cảnh thay đổi liên tục, khoảng cách dHash giữa hai khung kế tiếp rất lớn (> 10 bits), hầu như không khung nào bị lọc. Khi xe chạy rất chậm hoặc dừng đèn đỏ, khoảng cách dHash rất nhỏ (0 - 2 bits), do đó ngưỡng lọc trùng phát huy tác dụng rõ rệt nhất ở dải tốc độ thấp.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Dựa vào file bằng chứng `report/evidence/leakage.json`:
- Chia theo khung hình (per-frame split): Làm toàn bộ **26 / 26 trips** (100%) bị chia cắt (straddling), xuất hiện đồng thời ở cả tập train và tập test (`per_frame_split_trips_in_both_train_and_test = 26`).
- Chia theo chuyến đi (per-trip split): Số trip bị chia cắt bằng **0** (`per_trip_split_trips_straddling = 0`), các chuyến đi được phân lập độc lập hoàn toàn giữa các tập train, val và test.

Điều này gây ra hiện tượng rò rỉ dữ liệu (data leakage) làm điểm kiểm thử đẹp giả tạo vì:
- Trong cùng một chuyến đi, các khung hình kế tiếp nhau chỉ cách nhau 0.5 giây. Bối cảnh không gian xung quanh như điều kiện thời tiết, góc chiếu ánh sáng mặt trời, dãy nhà, loại cây ven đường và các phương tiện di chuyển phía trước gần như giống hệt nhau (tính tự tương quan không-thời gian cực kỳ cao).
- Khi chia per-frame, mô hình học máy sẽ "học vẹt" (overfitting) các khung hình ở train set và dễ dàng đoán đúng khung hình ở test set nhờ nền cảnh quen thuộc. Điểm số đánh giá trên tập test sẽ cao chót vót một cách giả tạo, nhưng khi triển khai trên một cung đường mới hoàn toàn, mô hình sẽ thất bại nặng nề vì không có khả năng tổng quát hóa (generalization).

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Tại sao `dataset_version` là băm nội dung (content-addressed hash):
- Tính tất định (determinism) và tính bất biến (immutability): Phiên bản được sinh ra bằng hàm băm SHA-256 từ danh sách chuỗi kết hợp `frame_id|split`. Nếu dữ liệu đầu vào và kết quả phân chia không thay đổi, mã băm sinh ra luôn luôn là duy nhất và cố định (`4140a455bef9`).
- Khắc phục nhược điểm của timestamp và số tự tăng: Nếu dùng ngày giờ hoặc số đếm, mỗi lần chạy lại pipeline (kể cả khi không có dữ liệu mới) sẽ tạo ra một version mới vô nghĩa, gây rối loạn cho việc quản lý mô hình (MLOps model lineage tracking).
- Hai lần chạy liên tiếp với cùng dữ liệu và tham số sẽ cho ra kết quả `dataset_version` hoàn toàn trùng khớp nhau từng ký tự.

Tại sao phiên bản của tôi khác bạn bè:
- Pipeline sử dụng kỹ thuật trộn muối (salting) bằng mã số sinh viên: `LAB_STUDENT_ID` (`2A202602987`) được nối vào `trip_id` trước khi băm MD5 để xác định thứ tự phân chia train/val/test: `md5(trip_id || salt)`.
- Do mỗi sinh viên có một MSSV duy nhất, thứ tự gán các trip vào train, val, test của từng bạn sẽ khác nhau ngẫu nhiên nhưng có tính tất định, dẫn đến tập hợp `frame_id|split` khác nhau và tạo ra mã `dataset_version` riêng biệt cho từng sinh viên.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Dựa vào trường `lineage` trong `lake/gold/datasets/4140a455bef9/manifest.json`:
Dataset `4140a455bef9` được tạo từ chính xác các phiên bản dữ liệu sau:
- Bảng Bronze:
  - `bronze.raw_telemetry_a`: version 0
  - `bronze.raw_telemetry_b`: version 0
  - `bronze.trips_raw`: version 0
  - `bronze.video_files`: version 1
  - Video gốc SHA-256:
    - `drive_a`: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`
    - `drive_b`: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`
- Bảng Silver:
  - `silver.frames`: version 25
  - `silver.telemetry`: version 0
  - `silver.frames_flagged`: version 0
  - `silver.frame_telemetry`: version 0

Nếu một kỹ sư hỏi lại sau 3 tháng, các thông tin sau giúp tái tạo lại 100% đúng dataset này:
1. Version cụ thể của từng Delta table ghi nhận trong commit log (Time Travel version), cho phép truy vấn lại chính xác trạng thái dữ liệu tại thời điểm tạo dataset.
2. Mã băm `input_video_sha256` của các video thô đầu vào để xác thực file video gốc không bị sửa đổi.
3. Mã sinh viên `LAB_STUDENT_ID` (`2A202602987`) làm seed/salt và bộ quy tắc lọc chất lượng trong `rules`.
4. File `manifest.parquet` với mã băm toàn vẹn `manifest_sha256: a569d937c26c6b21ae8591b66dc36bc55f3a7479d3adb4908701e2b7fe57510f` lưu vết toàn bộ danh sách 1019 khung hình cùng phân bổ split tương ứng.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Quá trình mô phỏng sự cố và phục hồi đối với trip `trip-04868`:
- Khi gán `LAB_FAIL_ONCE=trip-04868` trong cấu hình môi trường, tác vụ trích xuất khung hình phân tán `silver_frames` (tương ứng mapped task index 13 của `trip-04868`) ném ngoại lệ mô phỏng và chuyển sang màu đỏ (FAILED).
- Tổng cộng có **1 task đỏ** (`silver_frames`), và toàn bộ **5 task downstream** phụ thuộc phía sau (`dbt_silver_gold`, `publish_lake`, `export_gold_dataset`, `publish_warehouse`, `run_experiments`) chuyển sang màu cam (`upstream_failed`).
- Phục hồi: Xoá bỏ biến `LAB_FAIL_ONCE` khỏi `.env`, sau đó truy cập Airflow UI, chọn task `silver_frames` bị lỗi và bấm nút "Clear" (kèm downstream). Task chạy lại thành công (màu xanh lá) và kéo theo toàn bộ các task downstream hoàn thành xuất sắc.
- Dữ liệu: Nhờ cơ chế `overwrite` theo partition của Delta Lake, partition của `trip-04868` được ghi đè nguyên vẹn, số dòng của trip giữ đúng 43 khung hình, không bị nhân đôi dữ liệu. `dataset_version` cuối cùng vẫn giữ nguyên chuẩn xác là `4140a455bef9`.
- Bằng chứng: Ảnh chụp log task sập `report/screenshots/task_crash_log.png` ghi rõ lỗi `RuntimeError: LAB_FAIL_ONCE triggered for trip trip-04868`, và ảnh `report/screenshots/airflow_graph.png` thể hiện toàn bộ DAG xanh hoàn toàn sau khi chạy lại.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Khi mở rộng quy mô lên 500 xe mỗi ngày (tương đương hàng nghìn giờ video và Terabyte dữ liệu mỗi ngày), 3 thành phần sẽ quá tải và vỡ đầu tiên:
1. **Trích xuất video cục bộ trên một máy (Local OpenCV/FFmpeg single-node worker)**:
   - Điểm nghẽn: Giải mã hàng trăm video 4K/1080p bằng CPU trên một máy chủ Airflow worker duy nhất sẽ làm cạn kiệt tài nguyên CPU, RAM và gây nghẽn cổ chai disk I/O nghiêm trọng.
   - Giải pháp thay thế: Chuyển sang kiến trúc xử lý video phân tán (Distributed Video Processing) sử dụng Ray Cluster hoặc Apache Spark kết hợp card đồ họa GPU (NVDEC hardware acceleration) để giải mã song song, nén và đẩy trực tiếp các frame lên Object Storage (S3/MinIO).
2. **Engine tính toán DuckDB cục bộ (Single-node Lakehouse)**:
   - Điểm nghẽn: DuckDB là in-process database lưu trên một file đơn lẻ (`lakehouse.duckdb`). Khi hàng trăm worker cùng truy cập, DuckDB sẽ bị giới hạn bởi tài nguyên của 1 máy chủ vật lý và không hỗ trợ ghi đồng thời phân tán (concurrent distributed writes).
   - Giải pháp thay thế: Chuyển đổi engine phân tích sang Distributed Lakehouse Engine quy mô lớn như Apache Spark / Databricks, Trino hoặc StarRocks chạy trên nền bảng mở Delta Lake / Apache Iceberg.
3. **Bộ điều phối Airflow LocalExecutor và cơ sở dữ liệu Metadata đơn lẻ**:
   - Điểm nghẽn: Việc quản lý hàng trăm nghìn task instances mỗi ngày với LocalExecutor sẽ làm sập hàng đợi tác vụ và cơ sở dữ liệu PostgreSQL của Airflow.
   - Giải pháp thay thế: Nâng cấp kiến trúc Airflow sang KubernetesExecutor hoặc CeleryExecutor với Message Queue chuyên dụng (RabbitMQ/Redis/Kafka), cho phép các worker tự động co giãn linh hoạt (auto-scaling pods) theo nhu cầu tải.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
SELECT 
    t.drive_id,
    t.trip_id,
    ROUND(AVG(ft.speed_mps)::numeric, 2) AS avg_speed_mps,
    ROUND(MAX(ft.speed_mps)::numeric, 2) AS max_speed_mps,
    COUNT(ft.frame_id) AS total_valid_frames
FROM marts.mart_trip_summary t
JOIN marts.mart_speed_coverage ft ON t.trip_id = ft.trip_id
GROUP BY t.drive_id, t.trip_id
ORDER BY avg_speed_mps DESC;
```

Ý nghĩa phân tích:
Câu truy vấn này tổng hợp và phân tích mối tương quan giữa vận tốc trung bình, vận tốc tối đa và số lượng khung hình hợp lệ thu thập được trong từng chuyến đi của `drive_a` và `drive_b`. Kết quả giúp đội ngũ MLOps đánh giá chất lượng dữ liệu: phát hiện những chuyến đi có tốc độ di chuyển quá thấp (thời gian dừng chờ đèn đỏ/kẹt xe lớn dẫn đến nhiều khung trùng lặp tiềm ẩn) hoặc các chuyến đi có dải vận tốc cao (tương ứng với điều kiện lái xe trên cao tốc, cung cấp các khung hình quan trọng để huấn luyện mô hình bám làn và nhận diện biển báo tầm xa).

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- Điều bất ngờ nhất: Cơ chế phân chia split kết hợp "muối" MSSV và content-addressed versioning đem lại tính tất định tuyệt đối và khả năng tái lập (reproducibility) vô cùng chuẩn xác cho toàn bộ quy trình MLOps.
- Điều còn chưa rõ: Cách thức tối ưu hóa chi phí lưu trữ và hiệu năng truy vấn khi bảng Delta Lake phình to lên hàng triệu file Parquet nhỏ (bài toán small-file problem và cơ chế auto-compaction trong môi trường sản xuất).
- Lỗi dữ liệu pipeline chưa bắt được: Hiện tượng camera bị chói lóa ngược sáng mạnh (lens flare/direct sunlight) hoặc bị bám bẩn cục bộ (vệt nước mưa, bụi bẩn bám trên một góc kính). Các khung hình này vẫn có độ nét Laplacian cao (không bị mờ), không bị tối hoàn toàn và không trùng dHash, nhưng che khuất các làn đường thực tế, làm giảm chất lượng huấn luyện mô hình.

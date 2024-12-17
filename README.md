# 🚀 **Arsenal & ManCity Player Data Pipeline to PostgreSQL**

## 📋 **Deskripsi Proyek**
Proyek ini adalah pipeline **ETL** sederhana menggunakan **Apache Airflow** yang mengunduh data pemain sepak bola dari **MinIO** (object storage) dan memuatnya ke dalam database **PostgreSQL**. Data pemain Arsenal dan Manchester City dikelola secara paralel menggunakan **Airflow DAG**.

---

## 🛠️ **Fitur Utama**
1. **Unduh File JSON dari MinIO**:  
   Pipeline menggunakan MinIO sebagai sumber data dan mengunduh file JSON untuk data pemain Arsenal dan Manchester City.

2. **Muat Data ke PostgreSQL**:  
   Data pemain di-load ke database PostgreSQL dengan handling `upsert` (INSERT dan UPDATE jika data sudah ada).

3. **Parallel Processing**:  
   Menggunakan **Airflow** untuk memproses data pemain Arsenal dan Manchester City secara paralel.

4. **Penggunaan Connection String**:  
   Koneksi ke PostgreSQL diatur dengan connection string agar mudah diatur di konfigurasi.

---

## 🗂️ **Struktur Proyek**

---

## 🚀 **Teknologi yang Digunakan**
- **Apache Airflow**: Orkestrasi pipeline ETL.
- **MinIO**: Penyimpanan objek untuk file JSON.
- **PostgreSQL**: Database untuk menyimpan data pemain.
- **Python**: Bahasa pemrograman untuk transformasi dan load data.
- **Docker**: Manajemen container menggunakan Docker Compose.

---

## 🔧 **Prasyarat**
Sebelum menjalankan proyek ini, pastikan Anda memiliki:
1. **Docker & Docker Compose**  
   Instalasi: [Panduan Instalasi Docker](https://docs.docker.com/get-docker/)

2. **Apache Airflow** (opsional jika Anda menggunakan image Docker).

3. **Python 3.8+** (untuk debugging lokal).

---

## ⚙️ **Konfigurasi**
### **1. MinIO Configuration**
Pastikan MinIO berjalan dengan konfigurasi berikut:
- **Endpoint**: `http://minio:9000`
- **Access Key**: `admin`
- **Secret Key**: `password`
- **Bucket Names**: 
   - Arsenal → `arsenal-data`
   - ManCity → `mancity-data`

### **2. PostgreSQL Connection**
Database menggunakan **PostgreSQL** yang berjalan di **Docker**.  
Connection string untuk PostgreSQL:
```plaintext
postgresql://airflow:airflow@airflow-db:5432/premierleague
```

---

## 🚀 **Cara Menjalankan Proyek**

Ikuti langkah-langkah di bawah ini untuk menjalankan proyek:

### **1. Clone Repository**
Clone repository proyek ini ke komputer:
```bash
git clone https://github.com/username/repo-name.git
cd repo-name
```

### **2. Jalankan Docker Compose**

Untuk memulai semua layanan yang dibutuhkan, jalankan perintah berikut di terminal:

```bash
docker-compose up -d
```

### **3. Akses Airflow Webserver**

Setelah semua layanan berhasil dijalankan menggunakan Docker Compose, Anda dapat mengakses **Airflow Webserver** melalui browser dengan langkah-langkah berikut:

1. Buka browser dan kunjungi alamat berikut: http://localhost:8080

2. Masukkan kredensial default berikut untuk masuk ke Airflow:
- **Username**: `airflow`
- **Password**: `airflow`

3. Setelah berhasil login, Anda akan melihat halaman utama Airflow Webserver yang berisi daftar DAG (Directed Acyclic Graph) yang tersedia.

---

### **Memverifikasi DAG**
- Pada halaman utama, cari DAG dengan nama: arsenal_mancity_player_to_postgres

- Klik **Toggle** untuk mengaktifkan DAG tersebut.
- Untuk menjalankan DAG secara manual:
- Klik **Trigger DAG** (ikon play berwarna hijau) di sebelah kanan nama DAG.
- Periksa status eksekusi DAG melalui tab **Graph View** atau **Tree View**.

---





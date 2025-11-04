package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	firebase "firebase.google.com/go"
	"firebase.google.com/go/messaging"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// =================================================================
// DEKLARASI STRUKTUR & VARIABEL GLOBAL
// =================================================================

// Struktur data mentah dari genset (per detik)
type GensetData struct {
	Current     float64   `json:"current" firestore:"Current"`
	Energy      float64   `json:"energy" firestore:"Energy"`
	Frequency   float64   `json:"frequency" firestore:"Frequency"`
	PF          float64   `json:"pf" firestore:"PF"`
	Power       float64   `json:"power" firestore:"Power"`
	Time        string    `json:"time" firestore:"Time"`
	Voltage     float64   `json:"voltage" firestore:"Voltage"`
	Battery     float64   `json:"battery" firestore:"Battery"`
	BatteryCa   float64   `json:"battery_ca" firestore:"BatteryCa"`
	HosePress   float64   `json:"hose_press" firestore:"HosePress"`
	GasPressure float64   `json:"gas_press" firestore:"GasPressure"`
	CreatedAt   time.Time `json:"created_at" firestore:"CreatedAt"`
	TotalData   int       `json:"count" firestore:"TotalData"`
}

// Struktur ringkasan data per menit (Tidak digunakan secara langsung, tapi disertakan)
type GensetSummary struct {
	TimeMinute string    `json:"time_minute"`
	AvgVoltage float64   `json:"avg_voltage"`
	AvgCurrent float64   `json:"avg_current"`
	AvgPower   float64   `json:"avg_power"`
	AvgPF      float64   `json:"avg_pf"`
	AvgFreq    float64   `json:"avg_freq"`
	Energy     float64   `json:"energy"`
	CreatedAt  time.Time `json:"created_at"`
}

// Struktur notifikasi
type Notification struct {
	Title     string    `json:"title" firestore:"Title"`
	Body      string    `json:"body" firestore:"Body"`
	Type      string    `json:"type" firestore:"Type"`
	Value     float64   `json:"value" firestore:"Value"`
	CreatedAt time.Time `json:"created_at" firestore:"CreatedAt"`
}

var (
	client      *firestore.Client
	msgClient   *messaging.Client
	dataBuffer  []GensetData
	mu          sync.Mutex // Mutex untuk dataBuffer dan lastFlushed
	lastFlushed time.Time

	// 💡 Variabel In-Memory Cooldown
	lastNotifiedMap = make(map[string]time.Time) // map[notifType]lastSentTime
	cooldownMutex   sync.Mutex                   // Mutex khusus untuk lastNotifiedMap
)

const notificationCooldown = 3 * time.Minute

// =================================================================
// FUNGSI NOTIFICATION COOLDOWN (IN-MEMORY)
// =================================================================

// isRecentlyNotified memeriksa apakah notifikasi dengan tipe tertentu baru saja dikirim
func isRecentlyNotified(notifType string) bool {
	cooldownMutex.Lock()
	defer cooldownMutex.Unlock()

	lastTime, ok := lastNotifiedMap[notifType]
	if !ok {
		return false // Belum pernah dikirim
	}

	return time.Since(lastTime) < notificationCooldown
}

// recordNotification mencatat waktu saat ini sebagai waktu terakhir notifikasi dikirim.
func recordNotification(notifType string) {
	cooldownMutex.Lock()
	defer cooldownMutex.Unlock()

	lastNotifiedMap[notifType] = time.Now()
}

// =================================================================
// FUNGSI UTAMA: CHECK CRITICAL
// =================================================================

func checkCritical(data GensetData) {
	ctx := context.Background()
	var notifications []Notification

	// Helper function untuk memfilter notifikasi berdasarkan cooldown (in-memory) dan merekamnya
	filterAndAppend := func(notif Notification) {
		// 1. Cek Cooldown
		if isRecentlyNotified(notif.Type) {
			log.Printf("ℹ️ Notifikasi %s diabaikan (cooldown aktif)", notif.Type)
			return
		}

		// 2. Lolos, rekam waktu kirim baru (lock/unlock di dalam fungsi)
		recordNotification(notif.Type)

		// 3. Tambahkan ke daftar kirim
		notifications = append(notifications, notif)
	}

	// --- Voltage ---
	if data.Voltage < 198 {
		filterAndAppend(Notification{
			Title: "⚠️ Tegangan Terlalu Rendah",
			Body:  fmt.Sprintf("PERINGATAN: Tegangan sekarang %.1f V output di bawah 198V (Undervoltage). Perangkat elektronik bisa rusak.", data.Voltage),
			Type:  "voltage_low",
			Value: data.Voltage,
		})
	}
	if data.Voltage > 231 {
		filterAndAppend(Notification{
			Title: "⚠️ Tegangan Terlalu Tinggi",
			Body:  fmt.Sprintf("BAHAYA: Tegangan sekarang %.1f V output melebihi 231V (Overvoltage)! Risiko kerusakan perangkat. Segera kurangi beban.", data.Voltage),
			Type:  "voltage_high",
			Value: data.Voltage,
		})
	}

	// --- Gas Pressure ---
	if data.GasPressure < 10 {
		filterAndAppend(Notification{
			Title: "⚠️ Tekanan Gas Terlalu Rendah",
			Body:  fmt.Sprintf("PERINGATAN: tekanan gas sekarang %.1f psi di bawah 10 psi. Aliran gas lemah, mesin bisa mati.", data.GasPressure),
			Type:  "gas_low",
			Value: data.GasPressure,
		})
	}
	if data.GasPressure > 100 {
		filterAndAppend(Notification{
			Title: "⚠️ Tekanan Gas Terlalu Tinggi",
			Body:  fmt.Sprintf("PERINGATAN: tekanan gas sekarang %.1f psi melebihi 100 psi. Risiko kebocoran tinggi. Periksa regulator!", data.GasPressure),
			Type:  "gas_high",
			Value: data.GasPressure,
		})
	}

	// --- Gas hose ---
	if data.HosePress < 0.4 {
		filterAndAppend(Notification{
			Title: "⚠️ Tekanan Selang Gas Terlalu Rendah",
			Body:  fmt.Sprintf("PERINGATAN: tekanan selang gas sekarang %.1f psi di bawah 0.4 psi. Aliran gas lemah, mesin bisa mati.", data.HosePress),
			Type:  "hose_gas_low",
			Value: data.HosePress,
		})
	}
	if data.HosePress > 2.2 {
		filterAndAppend(Notification{
			Title: "⚠️ Tekanan Selang Gas Terlalu Tinggi",
			Body:  fmt.Sprintf("PERINGATAN: tekanan selang gas sekarang %.1f psi melebihi 2.2 psi. Risiko kebocoran tinggi. Periksa regulator!", data.HosePress),
			Type:  "hose_gas_high",
			Value: data.HosePress,
		})
	}

	// --- Battery Temp ---
	if data.BatteryCa < 0.0 {
		filterAndAppend(Notification{
			Title: "⚠️ Baterai Terlalu Dingin",
			Body:  fmt.Sprintf("PERINGATAN: Suhu baterai sekarang %.1f°C di bawah 0°C. Performa baterai mungkin menurun.", data.BatteryCa),
			Type:  "battery_temp_low",
			Value: data.BatteryCa,
		})
	}
	if data.BatteryCa > 60.0 {
		filterAndAppend(Notification{
			Title: "⚠️ Baterai Terlalu Panas",
			Body:  fmt.Sprintf("BAHAYA: Suhu baterai sekarang %.1f°C melebihi 60°C! Risiko overheat dan kerusakan permanen.", data.BatteryCa),
			Type:  "battery_temp_high",
			Value: data.BatteryCa,
		})
	}

	// --- Battery Level ---
	if data.Battery < 20 {
		filterAndAppend(Notification{
			Title: "⚠️ Baterai Lemah",
			Body:  "PERINGATAN: Baterai di bawah 20%. Risiko gagal start tinggi. Segera isi daya.",
			Type:  "battery_low",
			Value: data.Battery,
		})
	}
	if data.Battery > 100 {
		filterAndAppend(Notification{
			Title: "⚠️ Baterai Error",
			Body:  "KRITIS: Pembacaan baterai di atas 100%. Terindikasi error pada sensor.",
			Type:  "battery_error",
			Value: data.Battery,
		})
	}

	// --- Current ---
	if data.Current > 12.3 {
		filterAndAppend(Notification{
			Title: "⚠️ Arus Terlalu Tinggi",
			Body:  fmt.Sprintf("BAHAYA: Arus sekarang %.1f melebihi 12.3 A! Beban berlebih.", data.Current),
			Type:  "current_high",
			Value: data.Current,
		})
	}

	// --- Frequency ---
	if data.Frequency < 49.5 {
		filterAndAppend(Notification{
			Title: "⚠️ Frekuensi Terlalu Rendah",
			Body:  fmt.Sprintf("PERINGATAN: Frekuensi sekarang %.2f Hz di bawah 49.5 Hz.", data.Frequency),
			Type:  "freq_low",
			Value: data.Frequency,
		})
	}
	if data.Frequency > 50.5 {
		filterAndAppend(Notification{
			Title: "⚠️ Frekuensi Terlalu Tinggi",
			Body:  fmt.Sprintf("PERINGATAN: Frekuensi sekarang %.2f Hz di atas 50.5 Hz.", data.Frequency),
			Type:  "freq_high",
			Value: data.Frequency,
		})
	}

	// --- Power ---
	if data.Power > 2200 {
		filterAndAppend(Notification{
			Title: "⚠️ Daya Terlalu Tinggi",
			Body:  fmt.Sprintf("BAHAYA: Daya %.1f melebihi 2200 W! Beban berlebih (Overload).", data.Power),
			Type:  "power_high",
			Value: data.Power,
		})
	}

	// Kirim semua notifikasi yang LULUS filter
	for _, n := range notifications {
		n.CreatedAt = time.Now()
		// Operasi I/O (Firestore & FCM) tetap dilakukan async
		go saveNotification(ctx, n)
		go sendNotification(ctx, n)
	}
}

// Simpan notifikasi ke Firestore
func saveNotification(ctx context.Context, notif Notification) {
	_, _, err := client.Collection("notifications").Add(ctx, notif)
	if err != nil {
		log.Printf("❌ Gagal simpan notifikasi: %v", err)
	}
}

// Kirim notifikasi ke topic all_users
func sendNotification(ctx context.Context, notif Notification) {
	message := &messaging.Message{
		Topic: "all_users",
		Notification: &messaging.Notification{
			Title: notif.Title,
			Body:  notif.Body,
		},
	}

	_, err := msgClient.Send(ctx, message)
	if err != nil {
		log.Printf("❌ Gagal kirim FCM: %v", err)
	} else {
		log.Printf("✅ Notifikasi terkirim: %s", notif.Title)
	}
}

// =================================================================
// FUNGSI HANDLER DAN UTILITY
// =================================================================

func main() {
	ctx := context.Background()

	// 🔑 Load kredensial Firebase
	sa := option.WithCredentialsFile("serviceFirebase.json")

	// 🔥 Inisialisasi Firebase app
	app, err := firebase.NewApp(ctx, nil, sa)
	if err != nil {
		log.Fatalf("Failed to init Firebase App: %v", err)
	}

	// 📘 Firestore client
	client, err = app.Firestore(ctx)
	if err != nil {
		log.Fatalf("Failed to create Firestore client: %v", err)
	}

	// 📱 Messaging client
	msgClient, err = app.Messaging(ctx)
	if err != nil {
		log.Fatalf("Failed to create Messaging client: %v", err)
	}

	defer client.Close()

	// Route HTTP
	http.HandleFunc("/insert", insertHandler)
	http.HandleFunc("/history", historyHandler)

	fmt.Println("🚀 Server running at :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// Handler untuk menerima data genset per detik
func insertHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	var data GensetData
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	data.CreatedAt = time.Now()

	// Cek apakah ada kondisi critical
	checkCritical(data)

	// Masukkan ke buffer (Dilindungi oleh mu)
	mu.Lock()
	dataBuffer = append(dataBuffer, data)
	mu.Unlock()

	// Jika sudah 1 menit sejak flush terakhir, lakukan penyimpanan summary
	if time.Since(lastFlushed) > time.Minute {
		// Flush buffer secara async
		go flushToFirestore()
		// Catat waktu flush (Dilindungi oleh mu)
		mu.Lock()
		lastFlushed = time.Now()
		mu.Unlock()
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "Buffered")
}

// Flush buffer ke Firestore dalam bentuk summary per menit
func flushToFirestore() {
	mu.Lock()
	defer mu.Unlock() // Mutex untuk melindungi dataBuffer

	if len(dataBuffer) == 0 {
		return
	}

	// Mengambil nilai terakhir dari dataBuffer untuk disimpan sebagai ringkasan menit
	var totalV, totalC, totalP, totalPF, totalF, totalE, lastGasPress, lastGasHose, lastBattery, LastBatteryTemp float64
	for _, d := range dataBuffer {
		// Mengambil nilai terakhir (yang paling baru)
		totalV = d.Voltage
		totalC = d.Current
		totalP = d.Power
		totalPF = d.PF
		totalF = d.Frequency
		totalE = d.Energy
		lastGasHose = d.HosePress
		lastGasPress = d.GasPressure
		lastBattery = d.Battery
		LastBatteryTemp = d.BatteryCa
	}

	count := len(dataBuffer)
	now := time.Now()

	summary := GensetData{
		Time:        now.Format("15:04"),
		Voltage:     totalV,
		Current:     totalC,
		Power:       totalP,
		PF:          totalPF,
		Frequency:   totalF,
		Energy:      totalE,
		Battery:     lastBattery,
		BatteryCa:   LastBatteryTemp,
		HosePress:   lastGasHose,
		GasPressure: lastGasPress,
		CreatedAt:   now,
		TotalData:   count,
	}

	ctx := context.Background()
	docID := now.Format("2006-01-02-15-04") // YYYY-MM-DD-HH-MM

	docRef := client.Collection("genset_history").Doc(docID)

	_, err := docRef.Set(ctx, summary)

	if err != nil {
		log.Println("❌ Failed to save summary:", err)
	} else {
		log.Printf("✅ Saved summary %s (%d samples) to ID: %s\n", now, count, docID)
	}

	dataBuffer = nil
}

// Handler ambil data summary berdasarkan tanggal (misal untuk grafik)
func historyHandler(w http.ResponseWriter, r *http.Request) {
	dateStr := r.URL.Query().Get("date")
	if dateStr == "" {
		dateStr = time.Now().Format("2006-01-02")
	}

	// Tentukan rentang waktu untuk hari tersebut
	startOfDay, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		http.Error(w, "Invalid date format", http.StatusBadRequest)
		return
	}
	endOfDay := startOfDay.Add(24 * time.Hour)

	ctx := context.Background()

	// Query dokumen berdasarkan bidang CreatedAt (Timestamp)
	docs, err := client.Collection("genset_history").
		Where("CreatedAt", ">=", startOfDay).
		Where("CreatedAt", "<", endOfDay).
		OrderBy("CreatedAt", firestore.Asc).
		Documents(ctx).
		GetAll()

	if err != nil {
		// Pengecekan NotFound status GRPC di Firestore SDK
		if status.Code(err) == codes.NotFound {
			http.Error(w, "History data not found for this date.", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to fetch history: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var results []GensetData
	for _, doc := range docs {
		var s GensetData
		doc.DataTo(&s)
		results = append(results, s)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

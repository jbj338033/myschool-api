package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/swagger"
	"github.com/joho/godotenv"
	_ "myschool-api/docs"
)

type SchoolInfo struct {
	SchoolCode string `json:"SD_SCHUL_CODE"`
	OrgCode    string `json:"ATPT_OFCDC_SC_CODE"`
	SchoolName string `json:"SCHUL_NM"`
	Address    string `json:"ORG_RDNMA"`
	Type       string `json:"SCHUL_KND_SC_NM"`
}

type MealInfo struct {
	Date       string `json:"MLSV_YMD"`
	MealType   string `json:"MMEAL_SC_NM"`
	MenuDetail string `json:"DDISH_NM"`
	Calorie    string `json:"CAL_INFO"`
}

type TimetableInfo struct {
	Date      string `json:"ALL_TI_YMD"`
	Period    string `json:"PERIO"`
	Subject   string `json:"ITRT_CNTNT"`
	Teacher   string `json:"TEACHER_NM,omitempty"`
	ClassRoom string `json:"CLRM_NM,omitempty"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type School struct {
	Code    string `json:"code"`
	OrgCode string `json:"org_code"`
	Name    string `json:"name"`
	Address string `json:"address"`
	Type    string `json:"type"`
}

type MealData struct {
	Menu     []string `json:"menu"`
	Calories float64  `json:"calories,omitempty"`
}

type Meal map[string]MealData

type Timetable []string

type Cache struct {
	mu    sync.RWMutex
	items map[string]cacheItem
	ttl   time.Duration
}

type cacheItem struct {
	value      interface{}
	expiration time.Time
}

func NewCache(ttl time.Duration) *Cache {
	cache := &Cache{
		items: make(map[string]cacheItem),
		ttl:   ttl,
	}

	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			cache.cleanup()
		}
	}()

	return cache
}

func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, ok := c.items[key]
	if !ok || time.Now().After(item.expiration) {
		return nil, false
	}
	return item.value, true
}

func (c *Cache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items[key] = cacheItem{
		value:      value,
		expiration: time.Now().Add(c.ttl),
	}
}

func (c *Cache) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, item := range c.items {
		if now.After(item.expiration) {
			delete(c.items, key)
		}
	}
}

type SchoolSearchData struct {
	School            School
	InitialConsonants string
}

type NEISClient struct {
	apiKey      string
	cache       *Cache
	client      *http.Client
	schools     []SchoolSearchData
	mu          sync.RWMutex
	lastRefresh time.Time
	isLoading   bool
}

func NewNEISClient(apiKey string, cache *Cache) *NEISClient {
	return &NEISClient{
		apiKey: apiKey,
		cache:  cache,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		schools: make([]SchoolSearchData, 0),
	}
}

func getInitialConsonant(r rune) string {
	if r < 0xAC00 || r > 0xD7A3 {
		return string(r)
	}

	initials := []string{"ㄱ", "ㄲ", "ㄴ", "ㄷ", "ㄸ", "ㄹ", "ㅁ", "ㅂ", "ㅃ", "ㅅ", "ㅆ", "ㅇ", "ㅈ", "ㅉ", "ㅊ", "ㅋ", "ㅌ", "ㅍ", "ㅎ"}
	index := int(r - 0xAC00)
	initialIndex := index / (21 * 28)
	return initials[initialIndex]
}

func extractInitialConsonants(text string) string {
	result := ""
	for _, r := range text {
		if r >= 0xAC00 && r <= 0xD7A3 {
			result += getInitialConsonant(r)
		} else if r != ' ' {
			result += string(r)
		}
	}
	return result
}

func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func (n *NEISClient) LoadAllSchools() error {
	return n.RefreshSchools()
}

func (n *NEISClient) RefreshSchools() error {
	n.mu.Lock()
	if n.isLoading {
		n.mu.Unlock()
		return nil
	}
	n.isLoading = true
	n.mu.Unlock()

	defer func() {
		n.mu.Lock()
		n.isLoading = false
		n.mu.Unlock()
	}()

	regions := []struct {
		code string
		name string
	}{
		{"B10", "서울"},
		{"C10", "부산"},
		{"D10", "대구"},
		{"E10", "인천"},
		{"F10", "광주"},
		{"G10", "대전"},
		{"H10", "울산"},
		{"I10", "세종"},
		{"J10", "경기"},
		{"K10", "강원"},
		{"M10", "충북"},
		{"N10", "충남"},
		{"P10", "전북"},
		{"Q10", "전남"},
		{"R10", "경북"},
		{"S10", "경남"},
		{"T10", "제주"},
		{"V10", "재외"},
	}

	type regionResult struct {
		schools []SchoolSearchData
		region  string
		count   int
	}

	resultChan := make(chan regionResult, len(regions))
	var wg sync.WaitGroup

	for _, r := range regions {
		wg.Add(1)
		go func(region struct{ code, name string }) {
			defer wg.Done()
			schools := n.loadRegionSchools(region.code, region.name)
			resultChan <- regionResult{
				schools: schools,
				region:  region.code,
				count:   len(schools),
			}
		}(r)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	allSchools := make([]SchoolSearchData, 0, 15000)
	totalLoaded := 0
	for result := range resultChan {
		allSchools = append(allSchools, result.schools...)
		totalLoaded += result.count
		if result.count > 0 {
			slog.Info("loaded region", "region", result.region, "schools", result.count)
		}
	}

	n.mu.Lock()
	n.schools = allSchools
	n.lastRefresh = time.Now()
	n.mu.Unlock()

	slog.Info("total schools loaded", "count", totalLoaded)
	return nil
}

func (n *NEISClient) loadRegionSchools(regionCode, regionName string) []SchoolSearchData {
	var schools []SchoolSearchData

	for page := 1; ; page++ {
		baseURL := "https://open.neis.go.kr/hub/schoolInfo"
		params := url.Values{}
		params.Set("KEY", n.apiKey)
		params.Set("Type", "json")
		params.Set("pIndex", fmt.Sprintf("%d", page))
		params.Set("pSize", "1000")
		params.Set("ATPT_OFCDC_SC_CODE", regionCode)

		resp, err := n.client.Get(fmt.Sprintf("%s?%s", baseURL, params.Encode()))
		if err != nil {
			break
		}

		var result struct {
			SchoolInfo []interface{} `json:"schoolInfo"`
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if err := json.Unmarshal(body, &result); err != nil {
			break
		}

		if len(result.SchoolInfo) < 2 {
			break
		}

		rowData, ok := result.SchoolInfo[1].(map[string]interface{})
		if !ok {
			break
		}

		rows, ok := rowData["row"].([]interface{})
		if !ok || len(rows) == 0 {
			break
		}

		for _, rowItem := range rows {
			row, ok := rowItem.(map[string]interface{})
			if !ok {
				continue
			}

			school := School{
				Code:    getString(row, "SD_SCHUL_CODE"),
				OrgCode: getString(row, "ATPT_OFCDC_SC_CODE"),
				Name:    getString(row, "SCHUL_NM"),
				Address: getString(row, "ORG_RDNMA"),
				Type:    getString(row, "SCHUL_KND_SC_NM"),
			}
			if school.Code != "" && school.Name != "" {
				schools = append(schools, SchoolSearchData{
					School:            school,
					InitialConsonants: extractInitialConsonants(school.Name),
				})
			}
		}

		if len(rows) < 1000 {
			break
		}
	}

	return schools
}

func (n *NEISClient) SearchSchools(query string) ([]School, error) {
	cacheKey := fmt.Sprintf("search:%s", query)
	if cached, ok := n.cache.Get(cacheKey); ok {
		return cached.([]School), nil
	}

	n.mu.RLock()
	defer n.mu.RUnlock()

	if len(n.schools) == 0 {
		go n.LoadAllSchools()

		baseURL := "https://open.neis.go.kr/hub/schoolInfo"
		params := url.Values{}
		params.Set("KEY", n.apiKey)
		params.Set("Type", "json")
		params.Set("pSize", "100")
		params.Set("SCHUL_NM", "*"+query+"*")

		resp, err := n.client.Get(fmt.Sprintf("%s?%s", baseURL, params.Encode()))
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		var result struct {
			SchoolInfo []struct {
				Row []SchoolInfo `json:"row"`
			} `json:"schoolInfo"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, err
		}

		schools := make([]School, 0)
		if len(result.SchoolInfo) > 0 && len(result.SchoolInfo[0].Row) > 0 {
			for _, info := range result.SchoolInfo[0].Row {
				schools = append(schools, School{
					Code:    info.SchoolCode,
					OrgCode: info.OrgCode,
					Name:    info.SchoolName,
					Address: info.Address,
					Type:    info.Type,
				})
			}
		}
		return schools, nil
	}

	queryLower := strings.ToLower(query)
	queryInitial := extractInitialConsonants(query)
	queryRunes := []rune(query)

	type scoredSchool struct {
		school School
		score  int
	}

	var results []scoredSchool
	isInitialQuery := true

	for _, r := range queryRunes {
		if r >= 0xAC00 && r <= 0xD7A3 {
			isInitialQuery = false
			break
		} else if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			isInitialQuery = false
		}
	}

	for _, data := range n.schools {
		nameLower := strings.ToLower(data.School.Name)
		score := 0

		if data.School.Name == query {
			score = 10000
		} else if nameLower == queryLower {
			score = 9000
		} else if strings.HasPrefix(data.School.Name, query) {
			score = 8500
		} else if strings.HasPrefix(nameLower, queryLower) {
			score = 8000
		} else if isInitialQuery && strings.HasPrefix(data.InitialConsonants, query) {
			score = 7500
		} else if !isInitialQuery && strings.HasPrefix(data.InitialConsonants, queryInitial) {
			score = 7000
		} else if strings.Contains(data.School.Name, query) {
			score = 6500 - strings.Index(data.School.Name, query)
		} else if strings.Contains(nameLower, queryLower) {
			score = 6000 - strings.Index(nameLower, queryLower)
		} else if isInitialQuery && strings.Contains(data.InitialConsonants, query) {
			score = 5500 - strings.Index(data.InitialConsonants, query)
		} else if !isInitialQuery && strings.Contains(data.InitialConsonants, queryInitial) {
			score = 5000 - strings.Index(data.InitialConsonants, queryInitial)
		}

		if len(query) == 1 && score > 0 && !strings.HasPrefix(data.School.Name, query) && !strings.HasPrefix(data.InitialConsonants, query) {
			score = score / 2
		}

		if score > 0 {
			results = append(results, scoredSchool{
				school: data.School,
				score:  score,
			})
		}
	}

	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[i].score < results[j].score {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	schools := make([]School, 0, 100)
	for i, r := range results {
		if i >= 100 {
			break
		}
		schools = append(schools, r.school)
	}

	n.cache.Set(cacheKey, schools)
	return schools, nil
}

func (n *NEISClient) GetMeals(orgCode, schoolCode, date string) (Meal, error) {
	cacheKey := fmt.Sprintf("meals:%s:%s:%s", orgCode, schoolCode, date)
	if cached, ok := n.cache.Get(cacheKey); ok {
		return cached.(Meal), nil
	}

	baseURL := "https://open.neis.go.kr/hub/mealServiceDietInfo"
	params := url.Values{}
	params.Set("KEY", n.apiKey)
	params.Set("Type", "json")
	params.Set("ATPT_OFCDC_SC_CODE", orgCode)
	params.Set("SD_SCHUL_CODE", schoolCode)
	params.Set("MLSV_YMD", date)

	resp, err := n.client.Get(fmt.Sprintf("%s?%s", baseURL, params.Encode()))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var result struct {
		MealServiceDietInfo []interface{} `json:"mealServiceDietInfo"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	if len(result.MealServiceDietInfo) < 2 {
		return Meal{}, nil
	}

	rowData, ok := result.MealServiceDietInfo[1].(map[string]interface{})
	if !ok {
		return Meal{}, nil
	}

	rows, ok := rowData["row"].([]interface{})
	if !ok || len(rows) == 0 {
		return Meal{}, nil
	}

	meals := make(Meal)
	for _, rowItem := range rows {
		row, ok := rowItem.(map[string]interface{})
		if !ok {
			continue
		}

		menuDetail := getString(row, "DDISH_NM")
		menu := strings.Split(menuDetail, "<br/>")
		cleanMenu := make([]string, 0)
		for _, item := range menu {
			cleaned := strings.TrimSpace(item)
			for strings.Contains(cleaned, "(") {
				start := strings.Index(cleaned, "(")
				end := strings.Index(cleaned, ")")
				if start != -1 && end != -1 && end > start {
					cleaned = cleaned[:start] + cleaned[end+1:]
				} else {
					break
				}
			}
			cleaned = strings.TrimSpace(cleaned)
			if cleaned != "" {
				cleanMenu = append(cleanMenu, cleaned)
			}
		}

		mealType := getString(row, "MMEAL_SC_NM")
		caloriesStr := getString(row, "CAL_INFO")

		var calories float64
		if caloriesStr != "" {
			fmt.Sscanf(caloriesStr, "%f", &calories)
		}

		key := "lunch"
		if mealType == "조식" {
			key = "breakfast"
		} else if mealType == "석식" {
			key = "dinner"
		}

		meals[key] = MealData{
			Menu:     cleanMenu,
			Calories: calories,
		}
	}

	n.cache.Set(cacheKey, meals)
	return meals, nil
}

func (n *NEISClient) GetTimetable(orgCode, schoolCode, grade, class, date string) (Timetable, error) {
	cacheKey := fmt.Sprintf("timetable:%s:%s:%s:%s:%s", orgCode, schoolCode, grade, class, date)
	if cached, ok := n.cache.Get(cacheKey); ok {
		return cached.(Timetable), nil
	}

	baseURL := "https://open.neis.go.kr/hub/hisTimetable"
	params := url.Values{}
	params.Set("KEY", n.apiKey)
	params.Set("Type", "json")
	params.Set("ATPT_OFCDC_SC_CODE", orgCode)
	params.Set("SD_SCHUL_CODE", schoolCode)
	params.Set("GRADE", grade)
	params.Set("CLASS_NM", class)
	params.Set("TI_FROM_YMD", date)
	params.Set("TI_TO_YMD", date)

	resp, err := n.client.Get(fmt.Sprintf("%s?%s", baseURL, params.Encode()))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var result struct {
		HisTimetable []interface{} `json:"hisTimetable"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		baseURL = "https://open.neis.go.kr/hub/elsTimetable"
		resp, err = n.client.Get(fmt.Sprintf("%s?%s", baseURL, params.Encode()))
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		body, _ = io.ReadAll(resp.Body)
		var elResult struct {
			ElsTimetable []interface{} `json:"elsTimetable"`
		}

		if err := json.Unmarshal(body, &elResult); err != nil {
			return nil, err
		}

		result.HisTimetable = elResult.ElsTimetable
	}

	if len(result.HisTimetable) < 2 {
		return Timetable{}, nil
	}

	rowData, ok := result.HisTimetable[1].(map[string]interface{})
	if !ok {
		return Timetable{}, nil
	}

	rows, ok := rowData["row"].([]interface{})
	if !ok || len(rows) == 0 {
		return Timetable{}, nil
	}

	timetable := make(Timetable, 0)
	for _, rowItem := range rows {
		row, ok := rowItem.(map[string]interface{})
		if !ok {
			continue
		}

		subject := getString(row, "ITRT_CNTNT")
		timetable = append(timetable, subject)
	}

	n.cache.Set(cacheKey, timetable)
	return timetable, nil
}

var startTime = time.Now()

// GetAllSchools godoc
// @Summary Get all schools
// @Description Get all schools
// @Tags Schools
// @Produce json
// @Success 200 {array} School
// @Router /schools [get]
func GetAllSchools(neis *NEISClient) fiber.Handler {
	return func(c *fiber.Ctx) error {
		allSchools := make([]School, 0, len(neis.schools))
		for _, data := range neis.schools {
			allSchools = append(allSchools, data.School)
		}
		return c.JSON(allSchools)
	}
}

// GetMeals godoc
// @Summary Get school meals
// @Description Get meal information for a specific school and date
// @Tags Meals
// @Param org_code query string true "Education office code"
// @Param school_code query string true "School code"
// @Param date query string false "Date (YYYYMMDD format)"
// @Produce json
// @Success 200 {object} Meal
// @Failure 400 {object} ErrorResponse
// @Router /meals [get]
func GetMeals(neis *NEISClient) fiber.Handler {
	return func(c *fiber.Ctx) error {
		orgCode := c.Query("org_code")
		schoolCode := c.Query("school_code")
		date := c.Query("date")

		if orgCode == "" || schoolCode == "" {
			return c.Status(fiber.StatusBadRequest).JSON(ErrorResponse{
				Error: "org_code and school_code are required",
			})
		}

		if date == "" {
			date = time.Now().Format("20060102")
		}

		meals, err := neis.GetMeals(orgCode, schoolCode, date)
		if err != nil {
			slog.Error("failed to get meals", "error", err)
			return c.Status(fiber.StatusInternalServerError).JSON(ErrorResponse{
				Error: "failed to get meals",
			})
		}

		return c.JSON(meals)
	}
}

// GetTimetables godoc
// @Summary Get school timetables
// @Description Get timetable for a specific school, grade, and class
// @Tags Timetables
// @Param org_code query string true "Education office code"
// @Param school_code query string true "School code"
// @Param grade query string true "Grade"
// @Param class query string true "Class"
// @Param date query string false "Date (YYYYMMDD format)"
// @Produce json
// @Success 200 {array} string
// @Failure 400 {object} ErrorResponse
// @Router /timetables [get]
func GetTimetables(neis *NEISClient) fiber.Handler {
	return func(c *fiber.Ctx) error {
		orgCode := c.Query("org_code")
		schoolCode := c.Query("school_code")
		grade := c.Query("grade")
		class := c.Query("class")
		date := c.Query("date")

		if orgCode == "" || schoolCode == "" || grade == "" || class == "" {
			return c.Status(fiber.StatusBadRequest).JSON(ErrorResponse{
				Error: "org_code, school_code, grade, and class are required",
			})
		}

		if date == "" {
			date = time.Now().Format("20060102")
		}

		timetables, err := neis.GetTimetable(orgCode, schoolCode, grade, class, date)
		if err != nil {
			slog.Error("failed to get timetables", "error", err)
			return c.Status(fiber.StatusInternalServerError).JSON(ErrorResponse{
				Error: "failed to get timetables",
			})
		}

		return c.JSON(timetables)
	}
}

// @title MySchool API
// @version 1.0
// @description Simple and clean school meal/timetable API server
// @host localhost:8080
// @BasePath /api/v1
// @schemes http https
func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	neisAPIKey := os.Getenv("NEIS_API_KEY")
	if neisAPIKey == "" {
		slog.Error("NEIS_API_KEY is required")
		os.Exit(1)
	}

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	var level slog.Level
	switch strings.ToLower(logLevel) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})))

	cache := NewCache(1 * time.Hour)
	neis := NewNEISClient(neisAPIKey, cache)

	go func() {
		slog.Info("loading all schools in background...")
		if err := neis.LoadAllSchools(); err != nil {
			slog.Error("failed to load schools", "error", err)
		}

		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()

		for range ticker.C {
			slog.Info("refreshing school data...")
			if err := neis.RefreshSchools(); err != nil {
				slog.Error("failed to refresh schools", "error", err)
			}
		}
	}()

	app := fiber.New(fiber.Config{
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}
			return c.Status(code).JSON(ErrorResponse{
				Error: err.Error(),
			})
		},
	})

	app.Use(logger.New())
	app.Use(recover.New())
	app.Use(cors.New())

	// @Summary Health check
	// @Description Get server health status
	// @Tags System
	// @Produce json
	// @Success 200 {object} map[string]interface{}
	// @Router /health [get]
	app.Get("/health", func(c *fiber.Ctx) error {
		neis.mu.RLock()
		schoolCount := len(neis.schools)
		lastRefresh := neis.lastRefresh
		isLoading := neis.isLoading
		neis.mu.RUnlock()

		return c.JSON(fiber.Map{
			"status":        "healthy",
			"schools_count": schoolCount,
			"last_refresh":  lastRefresh.Format(time.RFC3339),
			"is_loading":    isLoading,
			"uptime":        time.Since(startTime).String(),
		})
	})

	app.Get("/swagger/*", swagger.HandlerDefault)

	api := app.Group("/api/v1")

	api.Get("/schools", GetAllSchools(neis))
	api.Get("/meals", GetMeals(neis))
	api.Get("/timetables", GetTimetables(neis))

	slog.Info("server starting", "port", port)
	if err := app.Listen(":" + port); err != nil {
		slog.Error("server error", "error", err)
	}
}
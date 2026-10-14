package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"easy-chat/pkg/toolcall"
)

var cityCoordinates = map[string][2]float64{
	"北京": {39.9042, 116.4074},
	"上海": {31.2304, 121.4737},
	"广州": {23.1291, 113.2644},
	"深圳": {22.5431, 114.0579},
	"杭州": {30.2741, 120.1551},
	"西安": {34.3416, 108.9398},
	"成都": {30.5728, 104.0668},
	"武汉": {30.5928, 114.3055},
	"南京": {32.0603, 118.7969},
	"重庆": {29.5630, 106.5516},
}

var SupportedCities = []string{
	"北京", "上海", "广州", "深圳", "杭州", "西安", "成都", "武汉", "南京", "重庆",
}

func handleGetWeather(_ context.Context, args map[string]string, userHint string) (string, error) {
	city := toolcall.ResolveCity(args, userHint, SupportedCities)
	return getRealWeather(city), nil
}

func getRealWeather(city string) string {
	city = strings.TrimSpace(city)
	if city == "" {
		return "请告诉我你想查询哪个城市的天气，例如：" + joinNames(SupportedCities)
	}
	coords, ok := cityCoordinates[city]
	if !ok {
		return fmt.Sprintf("抱歉，暂不支持查询 %s 的天气，目前支持：%s", city, joinNames(SupportedCities))
	}

	lat, lon := coords[0], coords[1]
	url := fmt.Sprintf("https://api.open-meteo.com/v1/forecast?latitude=%.4f&longitude=%.4f&current_weather=true&timezone=auto", lat, lon)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Sprintf("查询 %s 天气失败: %v", city, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Sprintf("查询 %s 天气失败: HTTP %d", city, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Sprintf("读取 %s 天气数据失败: %v", city, err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Sprintf("解析 %s 天气数据失败: %v", city, err)
	}

	currentWeather, ok := result["current_weather"].(map[string]interface{})
	if !ok {
		return fmt.Sprintf("获取 %s 天气数据失败", city)
	}

	temperature, _ := currentWeather["temperature"].(float64)
	weatherCode, _ := currentWeather["weathercode"].(float64)
	windSpeed, _ := currentWeather["windspeed"].(float64)

	return fmt.Sprintf("%s当前天气：%s，气温 %.1f°C，风速 %.1f km/h",
		city, weatherDescription(weatherCode), temperature, windSpeed)
}

func weatherDescription(code float64) string {
	switch int(code) {
	case 0:
		return "晴朗"
	case 1, 2, 3:
		return "多云"
	case 45, 48:
		return "雾"
	case 51, 53, 55:
		return "毛毛雨"
	case 56, 57:
		return "冻雨"
	case 61, 63, 65:
		return "下雨"
	case 66, 67:
		return "冻雨"
	case 71, 73, 75:
		return "下雪"
	case 77:
		return "雪粒"
	case 80, 81, 82:
		return "阵雨"
	case 85, 86:
		return "阵雪"
	case 95, 96, 99:
		return "雷雨"
	default:
		return "未知天气"
	}
}

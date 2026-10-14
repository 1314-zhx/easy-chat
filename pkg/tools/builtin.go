package tools

func (r *Registry) registerBuiltin(opts Options) {
	r.register("get_weather", Def{
		Description: "获取指定城市的实时天气（支持：北京、上海、广州、深圳、杭州、西安、成都、武汉、南京、重庆）",
		Parameters:  map[string]string{"city": "城市名称，如：北京"},
		Aliases:     []string{"weather_query"},
	}, handleGetWeather)

	r.register("get_time", Def{
		Description: "获取当前服务器时间",
		Parameters:  map[string]string{},
		Aliases:     []string{"time_query"},
	}, handleGetTime)
}

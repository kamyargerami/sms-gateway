package config

import (
	"os"
	"strconv"
)

func GetSMSCost() int {
	costStr := os.Getenv("SMS_COST")
	if costStr == "" {
		return 10 // Default fallback
	}
	cost, err := strconv.Atoi(costStr)
	if err != nil {
		return 10
	}
	return cost
}

package main

import (
	"fmt"
	"math/rand"
	"time"
)

// 最少1分钱
const MinAmount = int64(1)

func main() {
	// 总金额100元(转为最小分单位)，发10个红包
	count, amount := int64(10), int64(10000)
	var res []float64

	remain := amount
	for i := int64(0); i < count; i++ {
		x := CalculateEnvelop(count-i, remain)
		remain -= x
		res = append(res, float64(x)/100)
	}

	fmt.Println(res)
}

// CalculateEnvelop 计算随机红包序列
// count - 红包数量
// amount - 总金额(单位：分)
func CalculateEnvelop(count, amount int64) int64 {
	if count == 1 {
		return amount
	}

	// 2倍均值范围，避免分散过大
	maxAvailable := ((amount - count*MinAmount) / count) * 2
	//fmt.Println(maxAvailable)

	rand.Seed(time.Now().UnixNano())
	x := rand.Int63n(maxAvailable) + MinAmount

	return x
}

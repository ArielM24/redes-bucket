package main

import (
	"fmt"
	"math/rand"
	"net"
	"sort"
	"sync"
	"time"
)

var nums []int16

const arraySize int = 3500
const maxRandom int = 999

func main() {
	nums = randomArray(arraySize)
	fmt.Println("Numero de cubetas:")
	var n int
	port := 2000
	fmt.Scanf("%d", &n)
	if n >= 3500 {
		n = 3500
	} else if n <= 0 {
		n = 1
	}
	buckets := makeBuckets(n)
	sortedNums := make([]int16, 0)
	var wg sync.WaitGroup
	bcn := make(chan []int16)
	for i := 0; i < n; i++ {
		var counter int
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int, bucket []int16) {
			//fmt.Println("goroutine:", i)
			nport := fmt.Sprintf("localhost:%d", (port + counter))

			serv, errs := net.Listen("tcp", nport)
			for errs != nil {
				nport = fmt.Sprintf("localhost:%d", (port + counter))
				counter++
				serv, errs = net.Listen("tcp", nport)
			}

			wg.Add(1)
			go handleServer(wg, serv, nport)

			conc, errc := net.Dial("tcp", nport)
			exitOnError(errc)
			wg.Add(1)
			go handleClient(wg, conc, nport, bucket, bcn, i)
			select {
			case b := <-bcn:
				sortedNums = append(sortedNums, b...)
			}
			defer wg.Done()
			counter++
		}(&wg, i, buckets[i])
	}
	wg.Wait()
	fmt.Println("Sorted Array:", sortedNums)
}

func randomArray(n int) []int16 {
	arr := make([]int16, n)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < n; i++ {
		arr[i] = int16(rnd.Intn(maxRandom))
	}
	return arr
}

func makeBuckets(n int) [][]int16 {
	ln := len(nums)
	buckets := make([][]int16, n)
	c := int16(arraySize / n)
	if c == 0 {
		c++
	}
	for i := 0; i < ln; i++ {
		index := int(nums[i] / c)
		if index >= n {
			index = n - 1
		}
		buckets[index] = append(buckets[index], nums[i])
	}
	return buckets
}

func exitOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func shortsToBytes(arr []int16) []byte {
	res := make([]byte, 0)
	for i := 0; i < len(arr); i++ {
		res = append(res, shortToBytes(arr[i])...)
	}
	return res
}

func shortToBytes(n int16) []byte {
	res := make([]byte, 2)
	for i := 0; i < 2; i++ {
		res[i] = byte((n >> uint(i*8)) & 255)
	}
	return res
}

func bytesToShorts(arr []byte) []int16 {
	res := make([]int16, 0)
	for i := 0; i < len(arr); i += 2 {
		res = append(res, bytesToShort([]byte{arr[i], arr[i+1]}))
	}
	return res
}

func bytesToShort(arr []byte) int16 {
	var num int16
	num = int16(int16(arr[0]) | (int16(arr[1]) << 8))
	return num
}

func handleServer(wg *sync.WaitGroup, serv net.Listener, port string) {
	fmt.Println("Server", port)
	con, errc := serv.Accept()
	exitOnError(errc)
	size := make([]byte, 2)
	con.Read(size)
	byteBucket := make([]byte, bytesToShort(size))
	con.Read(byteBucket)
	bucket := bytesToShorts(byteBucket)
	fmt.Println("Bucket server:", bucket)
	bucket = sortBucket(bucket)
	fmt.Println("Bucket sorted in server:", bucket)
	con.Write(shortsToBytes(bucket))
	defer wg.Done()
}

func handleClient(wg *sync.WaitGroup, conc net.Conn, port string, bucket []int16, bcn chan []int16, n int) {
	fmt.Println("Client", port)
	fmt.Println("Bucket client:", bucket)
	byteBucket := shortsToBytes(bucket)
	size := shortToBytes(int16(len(byteBucket)))
	conc.Write(size)
	conc.Write(byteBucket)
	conc.Read(byteBucket)
	sortedBucket := bytesToShorts(byteBucket)
	fmt.Println("Bucket sorted from server:", sortedBucket)
	time.Sleep((time.Second * time.Duration(n)) / 1000)
	bcn <- sortedBucket
	defer wg.Done()
}

func sortBucket(bucket []int16) []int16 {
	sort.SliceStable(bucket, func(i, j int) bool {
		return bucket[i] < bucket[j]
	})
	return bucket
}

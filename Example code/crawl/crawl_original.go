package main

import (
	"fmt"
	"sync"
)

type Fetcher1 interface {
	// Fetch1 returns the body of URL and
	// a slice of URLs found on that page.
	Fetch1(url string) (body string, urls []string, err error)
}

// Crawl1 uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Crawl1(url string, depth int, fetcher Fetcher1, fetched1 map[string]bool, exit chan bool, mu *sync.Mutex) {
	// TODO: Fetch URLs in parallel.
	// TODO: Don't fetch the same URL twice.
	// This implementation doesn't do either:
	if depth <= 0 {
		exit <- true
		return
	}
	mu.Lock()
	if fetched1[url] {
		exit <- true
		mu.Unlock()
		return
	}
	fetched1[url] = true
	mu.Unlock()
	body, urls, err := fetcher.Fetch1(url)
	if err != nil {
		fmt.Println(err)
		exit <- true
		return
	}
	fmt.Printf("found: %s %q\n", url, body)
	e := make(chan bool)
	for _, u := range urls {
		go func() {
			Crawl1(u, depth-1, fetcher, fetched1, e, mu)
		}()
	}
	// wait for all child gorountines to exit
	for i := 0; i < len(urls); i++ {
		<-e
	}
	exit <- true
	return
}

func main() {
	var mu sync.Mutex
	exit := make(chan bool)
	fetchedmap1 := make(map[string]bool)
	go Crawl1("https://golang.org/", 4, fetcher1, fetchedmap1, exit, &mu)
	<-exit
}

// fakeFetcher1 is Fetcher1 that returns canned results.
type fakeFetcher1 map[string]*fakeResult1

type fakeResult1 struct {
	body string
	urls []string
}

func (f fakeFetcher1) Fetch1(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher1 is a populated fakeFetcher1.
var fetcher1 = fakeFetcher1{
	"https://golang.org/": &fakeResult1{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult1{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult1{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult1{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.orlg/": &fakeResult1{
		"The Gol Programming Language",
		[]string{
			//"https://golang.orlg/pkg/",
			//"https://golang.orlg/cmd/",
		},
	},
}

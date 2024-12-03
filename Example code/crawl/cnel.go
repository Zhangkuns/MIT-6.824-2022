package main

import (
	"fmt"
	"sync"
)

/*
This solution uses channels to force each gorountines to wait for its child gorountines to exit.
*/

type SafeCounter struct {
	v   map[string]bool
	mux sync.Mutex
}

type Fetcher2 interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch2(url string) (body string, urls []string, err error)
}

var cnt SafeCounter = SafeCounter{v: make(map[string]bool)}

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Crawl2(url string, depth int, fetcher2 Fetcher2, exit chan bool) {
	// Fetch URLs in parallel.
	// Don't fetch the same URL twice.

	if depth <= 0 {
		exit <- true
		return
	}

	cnt.mux.Lock()
	_, ok := cnt.v[url]
	if ok == false {
		cnt.v[url] = true
		cnt.mux.Unlock()
	} else {
		exit <- true
		cnt.mux.Unlock()
		return
	}

	body, urls, err := fetcher2.Fetch2(url)
	if err != nil {
		fmt.Println(err)
		exit <- true
		return
	}
	fmt.Printf("found: %s %q\n", url, body)

	e := make(chan bool)
	for _, u := range urls {
		go Crawl2(u, depth-1, fetcher2, e)
	}

	// wait for all child gorountines to exit
	for i := 0; i < len(urls); i++ {
		<-e
	}
	exit <- true
}

func main() {
	exit := make(chan bool)
	go Crawl2("https://golang.org/", 4, fetcher2, exit)
	<-exit
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher2 map[string]*fakeResult2

type fakeResult2 struct {
	body string
	urls []string
}

func (f fakeFetcher2) Fetch2(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher2 = fakeFetcher2{
	"https://golang.org/": &fakeResult2{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult2{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult2{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult2{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}

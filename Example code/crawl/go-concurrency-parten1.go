package main

import (
	"fmt"
	"sync"
)

type Fetcher3 interface {
	// Fetch3 returns the body of URL and
	// a slice of URLs found on that page.
	Fetch3(url string) (body string, urls []string, err error)
}

// Crawl3 uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Crawl3(url string, depth int, fetcher Fetcher3, fetched3 map[string]bool, exit chan bool, mu *sync.Mutex) {
	// TODO: Fetch URLs in parallel.
	// TODO: Don't fetch the same URL twice.
	// This implementation doesn't do either:
	if depth <= 0 {
		return
	}
	mu.Lock()
	if fetched3[url] {
		mu.Unlock()
		return
	}
	fetched3[url] = true
	mu.Unlock()
	body, urls, err := fetcher.Fetch3(url)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("found: %s %q\n", url, body)
	for _, u := range urls {
		childExit := spawn(Crawl3, u, depth-1, fetcher, fetched3, mu)
		<-childExit // Wait for the child goroutine to finish
	}
	return
}

func spawn(f func(url string, depth int, fetcher Fetcher3, fetched3 map[string]bool, exit chan bool, mu *sync.Mutex),
	url string, depth int, fetcher Fetcher3, fetched3 map[string]bool, mu *sync.Mutex) chan bool {
	c := make(chan bool)
	go func() {
		defer close(c) // Ensure the channel is closed when the function is done
		f(url, depth, fetcher, fetched3, c, mu)
	}()
	return c
}

func main() {
	var mu sync.Mutex
	fetchedmap3 := make(map[string]bool)
	exit := spawn(Crawl3, "https://golang.org/", 4, fetcher3, fetchedmap3, &mu)
	<-exit
}

// fakeFetcher3 is Fetcher1 that returns canned results.
type fakeFetcher3 map[string]*fakeResult3

type fakeResult3 struct {
	body string
	urls []string
}

func (f fakeFetcher3) Fetch3(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher1 is a populated fakeFetcher1.
var fetcher3 = fakeFetcher3{
	"https://golang.org/": &fakeResult3{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult3{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult3{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult3{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.orlg/": &fakeResult3{
		"The Gol Programming Language",
		[]string{
			//"https://golang.orlg/pkg/",
			//"https://golang.orlg/cmd/",
		},
	},
}

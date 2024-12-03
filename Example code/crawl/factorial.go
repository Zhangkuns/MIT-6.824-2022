package main

func factorial(n int, f func(int)) {
	if n == 1 {
		f(1)
	} else {
		factorial(n-1, func(r int) {
			f(r * n)
		})
	}
}

func main() {
	factorial(5, func(r int) {
		println(r)
	})
}

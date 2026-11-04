//go:build (amd64 || arm64) && !gocroaring_cqueries

package gocroaring

func init() {
	setWorkBudget = func(b int) (restore func()) {
		c, i := goContainerBudget, goIntersectBudget
		goContainerBudget, goIntersectBudget = b, b
		return func() { goContainerBudget, goIntersectBudget = c, i }
	}
}

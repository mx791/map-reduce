package main

type MapReducer interface {
	Map(messages []string, serverCount int) ([]int, []string)
	Split(messages []string, serverCount int) [][]string
	ShuffleReceiver(messages []string)
	GetReduceData() string
	ReduceReceiver(messages []string)
	ReduceDone()
	Reset()
}

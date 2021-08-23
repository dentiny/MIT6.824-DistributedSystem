package mr

type TaskPhase int
const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type Task struct {
	Filename  string
	Phase     TaskPhase
	Seq       int
	NMap      int
	NReduce   int
}

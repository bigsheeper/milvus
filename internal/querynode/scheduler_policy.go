package querynode

import (
	"container/list"
)

type scheduleSQPolicy func(sqTasks *list.List, targetUsage int32) ([]sqTask, int32)

func defaultScheduleSQPolicy(sqTasks *list.List, targetUsage int32) ([]sqTask, int32) {
	var ret []sqTask
	usage := int32(0)
	for e := sqTasks.Front(); e != nil; e = e.Next() {
		t, _ := e.Value.(sqTask)
		tUsage := t.EstimateCpuUsage()
		if usage+tUsage > targetUsage {
			break
		}
		usage += tUsage
		sqTasks.Remove(e)
		ret = append(ret, t)
		break
	}
	return ret, usage
}

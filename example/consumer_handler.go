package main

import (
	"fmt"
	"time"
)

type ConsumerHandler struct{ started time.Time }

func (this *ConsumerHandler) Consume(sequence, remaining int64) {
	if sequence%Mod == 0 {
		finished := time.Now()
		fmt.Println(sequence, finished.Sub(this.started))
		this.started = time.Now()
	} else if sequence != ringBuffer[sequence&RingMask] {
		message := ringBuffer[sequence&RingMask]
		panic(fmt.Sprintf("Race condition--Sequence: %d, Message: %d\n", sequence, message))
	}
}

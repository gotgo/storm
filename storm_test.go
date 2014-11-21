package storm_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strconv"

	. "github.com/gotgo/storm"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Storm", func() {
	var target *Storm
	var tempDir string
	var responseBuffer *bytes.Buffer

	BeforeEach(func() {
		target = NewStorm()
		tempDir = os.TempDir()

		ci := &ConnectInfo{PidDir: tempDir}
		in, _ := json.Marshal(ci)

		buf := new(bytes.Buffer)
		fmt.Fprintln(buf, in)
		fmt.Fprintln(buf, "end")
		target.ExtIn = buf
		responseBuffer = new(bytes.Buffer)
		target.ExtOut = responseBuffer
		target.Run()
	})

	AfterEach(func() {
		target.End()
	})

	It("should connect", func() {
		output := ""
		fmt.Fscanln(responseBuffer, &output)
		p := ProcessId{}
		json.Unmarshal(responseBuffer.Bytes(), &p)

		pid := strconv.Itoa(os.Getpid())
		Expect(p.Pid).To(Equal(pid))

		filePath := path.Join(tempDir, p.Pid)
		_, err := os.Stat(filePath)
		Expect(os.IsNotExist(err)).To(BeFalse())
		os.Remove(filePath)
	})
})

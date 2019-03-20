package message

import (
	"encoding/base64"
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
)

var key = []byte("ajsgdfosgdlhsbadfhsd;ifhusdf87hs")
var nonce = []byte("234978fskdjfbsq5")
var logger = zap.NewNop()

var _ = Describe("streamio decoder tests", func() {

	It("streamio initialization must pass", func() {
		decodedKey := base64.StdEncoding.EncodeToString(key)
		sd, err := NewStreamioDecoder([]byte(decodedKey), logger, "", "")
		Ω(err).Should(BeNil())
		Ω(sd).ShouldNot(BeNil())
	})

	It("streamio decoder must pass", func() {
		encodedKey := base64.StdEncoding.EncodeToString(key)
		sd, _ := NewStreamioDecoder([]byte(encodedKey), logger, "", "")

		plainText := `{"project": "styx"}`
		encrypted := sd.gcm.Seal(nil, nonce, []byte(plainText), nil)

		msg := &stringMessage{
			msg: string(append(nonce, encrypted...)),
		}
		m, err := sd.DecodeMsg(msg)
		Ω(err).Should(BeNil())
		Ω(m).ShouldNot(BeNil())
		Ω(len(m.DecodedMessage)).Should(Equal(1))
		Ω(m.DecodedMessage[MsgPrefix].(map[string]interface{})["project"]).Should(Equal("styx"))

		plainText = `{"project":}`
		encrypted = sd.gcm.Seal(nil, nonce, []byte(plainText), nil)

		msg = &stringMessage{
			msg: string(append(nonce, encrypted...)),
		}
		m, err = sd.DecodeMsg(msg)
		Ω(err).ShouldNot(BeNil())
		Ω(m).Should(BeNil())

		plainText = `{"_updated": "2016-08-03"}`
		encrypted = sd.gcm.Seal(nil, nonce, []byte(plainText), nil)

		msg = &stringMessage{
			msg: string(append(nonce, encrypted...)),
		}
		m, err = sd.DecodeMsg(msg)
		Ω(err).Should(BeNil())
		Ω(m).ShouldNot(BeNil())
	})

	It("streamio decoder must fail", func() {
		encodedKey := base64.StdEncoding.EncodeToString(key)
		sd, _ := NewStreamioDecoder([]byte(encodedKey), logger, "", "")

		plainText := `{"project": "styx"}`
		encrypted := sd.gcm.Seal(nil, nonce, []byte(plainText), nil)

		msg := &stringMessage{
			msg: string(encrypted),
		}
		m, err := sd.DecodeMsg(msg)
		Ω(err).ShouldNot(BeNil())
		Ω(m).Should(BeNil())

		sd, err = NewStreamioDecoder([]byte("bad key"), logger, "", "")
		Ω(err).ShouldNot(BeNil())
		Ω(sd).Should(BeNil())

		sd, err = NewStreamioDecoder([]byte("dmVyeSBiYWQga2V5"), logger, "", "")
		Ω(err).ShouldNot(BeNil())
		Ω(sd).Should(BeNil())
	})

	Describe("get timestamp from streamio message", func() {
		It("report message age must fail", func() {
			s := `{"ts":"abc"}`
			m := make(map[string]interface{})
			json.Unmarshal([]byte(s), &m)
			ts, err := getTimestampFromStreamioMsg(m)
			var t time.Time
			Ω(err).Should(BeNil())
			Ω(ts).Should(Equal(t))
		})

		It("report message age for streamio must pass", func() {
			s := `{"_updated":"2016-07-12T22:54:39.978009+00:00","_created_at":"2016-07-12T22:54:39.977501+00:00","_dbname":"mez_shard940"}`
			m := make(map[string]interface{})
			json.Unmarshal([]byte(s), &m)
			ts, err := getTimestampFromStreamioMsg(m)
			t, _ := time.Parse(time.RFC3339, "2016-07-12T22:54:39.978009+00:00")
			Ω(err).Should(BeNil())
			Ω(ts).Should(Equal(t))
		})

		It("report message age for streamio must fail", func() {
			s := `{"_updated":"2016-07-12T22:5478009+00:00","_created_at":"2016-07-12T22:54:39.977501+00:00","_dbname":"mez_shard940"}`
			m := make(map[string]interface{})
			json.Unmarshal([]byte(s), &m)
			ts, err := getTimestampFromStreamioMsg(m)
			var t time.Time
			Ω(err).ShouldNot(BeNil())
			Ω(ts).Should(Equal(t))

			s = `{"_updated":1234567890,"_created_at":"2016-07-12T22:54:39.977501+00:00","_dbname":"mez_shard940"}`
			m = make(map[string]interface{})
			json.Unmarshal([]byte(s), &m)
			ts, err = getTimestampFromStreamioMsg(m)
			Ω(err).ShouldNot(BeNil())
			Ω(ts).Should(Equal(t))
		})
	})
})

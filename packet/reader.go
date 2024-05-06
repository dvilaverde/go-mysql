package packet

import "io"

var _ io.Reader = &PacketReader{}

type PacketReader struct {
	source   io.Reader
	sequence uint8
}

func NewPacketReader(src io.Reader) *PacketReader {
	return &PacketReader{
		source:   src,
		sequence: 0,
	}
}

func (w *PacketReader) Read(p []byte) (n int, err error) {
	return 0, nil
}

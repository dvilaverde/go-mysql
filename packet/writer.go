package packet

import "io"

var _ io.Writer = &PacketWriter{}

type PacketWriter struct {
	destination      io.Writer
	maxAllowedPacket int
	sequence         uint8
}

func NewPacketWriter(dst io.Writer, maxPacket int) *PacketWriter {
	return &PacketWriter{
		destination:      dst,
		maxAllowedPacket: maxPacket,
		sequence:         0,
	}
}

func (w *PacketWriter) Write(p []byte) (n int, err error) {
	return 0, nil
}

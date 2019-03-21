package types

import (
	"io"
	"time"
	"unsafe"
)

var (
	_ = unsafe.Sizeof(0)
	_ = io.ReadFull
	_ = time.Now()
)

type DocField struct {
	GeoHash string
	Lat     float64
	Lng     float64
}

func (d *DocField) Size() (s uint64) {

	{
		l := uint64(len(d.GeoHash))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	s += 16
	return
}
func (d *DocField) Marshal(buf []byte) ([]byte, error) {
	size := d.Size()
	{
		if uint64(cap(buf)) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}
	}
	i := uint64(0)

	{
		l := uint64(len(d.GeoHash))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		copy(buf[i+0:], d.GeoHash)
		i += l
	}
	{

		v := *(*uint64)(unsafe.Pointer(&(d.Lat)))

		buf[i+0+0] = byte(v >> 0)

		buf[i+1+0] = byte(v >> 8)

		buf[i+2+0] = byte(v >> 16)

		buf[i+3+0] = byte(v >> 24)

		buf[i+4+0] = byte(v >> 32)

		buf[i+5+0] = byte(v >> 40)

		buf[i+6+0] = byte(v >> 48)

		buf[i+7+0] = byte(v >> 56)

	}
	{

		v := *(*uint64)(unsafe.Pointer(&(d.Lng)))

		buf[i+0+8] = byte(v >> 0)

		buf[i+1+8] = byte(v >> 8)

		buf[i+2+8] = byte(v >> 16)

		buf[i+3+8] = byte(v >> 24)

		buf[i+4+8] = byte(v >> 32)

		buf[i+5+8] = byte(v >> 40)

		buf[i+6+8] = byte(v >> 48)

		buf[i+7+8] = byte(v >> 56)

	}
	return buf[:i+16], nil
}

func (d *DocField) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		d.GeoHash = string(buf[i+0 : i+0+l])
		i += l
	}
	{

		v := 0 | (uint64(buf[i+0+0]) << 0) | (uint64(buf[i+1+0]) << 8) | (uint64(buf[i+2+0]) << 16) | (uint64(buf[i+3+0]) << 24) | (uint64(buf[i+4+0]) << 32) | (uint64(buf[i+5+0]) << 40) | (uint64(buf[i+6+0]) << 48) | (uint64(buf[i+7+0]) << 56)
		d.Lat = *(*float64)(unsafe.Pointer(&v))

	}
	{

		v := 0 | (uint64(buf[i+0+8]) << 0) | (uint64(buf[i+1+8]) << 8) | (uint64(buf[i+2+8]) << 16) | (uint64(buf[i+3+8]) << 24) | (uint64(buf[i+4+8]) << 32) | (uint64(buf[i+5+8]) << 40) | (uint64(buf[i+6+8]) << 48) | (uint64(buf[i+7+8]) << 56)
		d.Lng = *(*float64)(unsafe.Pointer(&v))

	}
	return i + 16, nil
}

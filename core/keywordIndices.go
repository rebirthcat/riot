package core

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

type KeywordIndices struct {
	docIds      []uint64
	frequencies []float32
	locations   [][]int32
}

func (d *KeywordIndices) Size() (s uint64) {

	{
		l := uint64(len(d.docIds))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}

		s += 8 * l

	}
	{
		l := uint64(len(d.frequencies))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}

		s += 4 * l

	}
	{
		l := uint64(len(d.locations))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}

		for k0 := range d.locations {

			{
				l := uint64(len(d.locations[k0]))

				{

					t := l
					for t >= 0x80 {
						t >>= 7
						s++
					}
					s++

				}

				s += 4 * l

			}

		}

	}
	return
}
func (d *KeywordIndices) Marshal(buf []byte) ([]byte, error) {
	defer func() {
		if err := recover(); err != nil {
			return
		}
	}()
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
		l := uint64(len(d.docIds))

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
		for k0 := range d.docIds {

			{

				buf[i+0+0] = byte(d.docIds[k0] >> 0)

				buf[i+1+0] = byte(d.docIds[k0] >> 8)

				buf[i+2+0] = byte(d.docIds[k0] >> 16)

				buf[i+3+0] = byte(d.docIds[k0] >> 24)

				buf[i+4+0] = byte(d.docIds[k0] >> 32)

				buf[i+5+0] = byte(d.docIds[k0] >> 40)

				buf[i+6+0] = byte(d.docIds[k0] >> 48)

				buf[i+7+0] = byte(d.docIds[k0] >> 56)

			}

			i += 8

		}
	}
	{
		l := uint64(len(d.frequencies))

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
		for k0 := range d.frequencies {

			{

				v := *(*uint32)(unsafe.Pointer(&(d.frequencies[k0])))

				buf[i+0+0] = byte(v >> 0)

				buf[i+1+0] = byte(v >> 8)

				buf[i+2+0] = byte(v >> 16)

				buf[i+3+0] = byte(v >> 24)

			}

			i += 4

		}
	}
	{
		l := uint64(len(d.locations))

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
		for k0 := range d.locations {

			{
				l := uint64(len(d.locations[k0]))

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
				for k1 := range d.locations[k0] {

					{

						buf[i+0+0] = byte(d.locations[k0][k1] >> 0)

						buf[i+1+0] = byte(d.locations[k0][k1] >> 8)

						buf[i+2+0] = byte(d.locations[k0][k1] >> 16)

						buf[i+3+0] = byte(d.locations[k0][k1] >> 24)

					}

					i += 4

				}
			}

		}
	}
	return buf[:i+0], nil
}

func (d *KeywordIndices) Unmarshal(buf []byte) (uint64, error) {
	defer func() {
		if err := recover(); err != nil {
			return
		}
	}()
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
		if uint64(cap(d.docIds)) >= l {
			d.docIds = d.docIds[:l]
		} else {
			d.docIds = make([]uint64, l)
		}
		for k0 := range d.docIds {

			{

				d.docIds[k0] = 0 | (uint64(buf[i+0+0]) << 0) | (uint64(buf[i+1+0]) << 8) | (uint64(buf[i+2+0]) << 16) | (uint64(buf[i+3+0]) << 24) | (uint64(buf[i+4+0]) << 32) | (uint64(buf[i+5+0]) << 40) | (uint64(buf[i+6+0]) << 48) | (uint64(buf[i+7+0]) << 56)

			}

			i += 8

		}
	}
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
		if uint64(cap(d.frequencies)) >= l {
			d.frequencies = d.frequencies[:l]
		} else {
			d.frequencies = make([]float32, l)
		}
		for k0 := range d.frequencies {

			{

				v := 0 | (uint32(buf[i+0+0]) << 0) | (uint32(buf[i+1+0]) << 8) | (uint32(buf[i+2+0]) << 16) | (uint32(buf[i+3+0]) << 24)
				d.frequencies[k0] = *(*float32)(unsafe.Pointer(&v))

			}

			i += 4

		}
	}
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
		if uint64(cap(d.locations)) >= l {
			d.locations = d.locations[:l]
		} else {
			d.locations = make([][]int32, l)
		}
		for k0 := range d.locations {

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
				if uint64(cap(d.locations[k0])) >= l {
					d.locations[k0] = d.locations[k0][:l]
				} else {
					d.locations[k0] = make([]int32, l)
				}
				for k1 := range d.locations[k0] {

					{

						d.locations[k0][k1] = 0 | (int32(buf[i+0+0]) << 0) | (int32(buf[i+1+0]) << 8) | (int32(buf[i+2+0]) << 16) | (int32(buf[i+3+0]) << 24)

					}

					i += 4

				}
			}

		}
	}
	return i + 0, nil
}

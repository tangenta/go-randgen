package sqlgen

type ControlOption struct {
	MaxTableNum int
}

func DefaultControlOption() *ControlOption {
	return &ControlOption{
		MaxTableNum: 3,
	}
}

package sqlgen

type ControlOption struct {
	MaxTableNum      int
	StrictTransTable bool
}

func DefaultControlOption() *ControlOption {
	return &ControlOption{
		MaxTableNum:      1,
		StrictTransTable: true,
	}
}

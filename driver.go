package log

func NewLogDriver(logDriverType string, options interface{}) (driver Driver, err error) {
	switch logDriverType {
	case DriverTypeFs:
		if options == nil {
			options = &SeaweedFsLogDriverOptions{}
		}
		options, ok := options.(*SeaweedFsLogDriverOptions)
		if !ok {
			return driver, ErrInvalidType
		}
		driver, err = NewSeaweedFsLogDriver(options)
		if err != nil {
			return driver, err
		}
	case DriverTypeMongo:
		return driver, ErrNotImplemented
	case DriverTypeEs:
		return driver, ErrNotImplemented
	default:
		return driver, ErrInvalidType
	}
	return driver, nil
}

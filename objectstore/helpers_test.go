package objectstore

import "errors"

func isConditionConflict(err error) bool { return errors.Is(err, ErrConditionConflict) }

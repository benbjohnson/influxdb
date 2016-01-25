package influxql

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

// ErrUnknownCall is returned when operating on an unknown function call.
var ErrUnknownCall = errors.New("unknown call")

const (
	// MinTime is used as the minimum time value when computing an unbounded range.
	MinTime = int64(0)

	// MaxTime is used as the maximum time value when computing an unbounded range.
	// This time is Jan 1, 2050 at midnight UTC.
	MaxTime = int64(2524608000000000000)
)

// Iterator represents a generic interface for all Iterators.
// Most iterator operations are done on the typed sub-interfaces.
type Iterator interface {
	Close() error
}

// Iterators represents a list of iterators.
type Iterators []Iterator

// Close closes all iterators.
func (a Iterators) Close() error {
	for _, itr := range a {
		itr.Close()
	}
	return nil
}

// filterNonNil returns a slice of iterators that removes all nil iterators.
func (a Iterators) filterNonNil() []Iterator {
	other := make([]Iterator, 0, len(a))
	for _, itr := range a {
		if itr == nil {
			continue
		}
		other = append(other, itr)
	}
	return other
}

// NewMergeIterator returns an iterator to merge itrs into one.
func NewMergeIterator(inputs []Iterator, opt IteratorOptions) Iterator {
	inputs = Iterators(inputs).filterNonNil()
	if len(inputs) == 0 {
		return &nilFloatIterator{}
	}

	// Aggregate functions can use a more relaxed sorting so that points
	// within a window are grouped. This is much more efficient.
	switch input := inputs[0].(type) {
	case FloatIterator:
		return newFloatMergeIterator(newFloatIterators(inputs), opt)
	case IntegerIterator:
		return newIntegerMergeIterator(newIntegerIterators(inputs), opt)
	case StringIterator:
		return newStringMergeIterator(newStringIterators(inputs), opt)
	case BooleanIterator:
		return newBooleanMergeIterator(newBooleanIterators(inputs), opt)
	default:
		panic(fmt.Sprintf("unsupported merge iterator type: %T", input))
	}
}

// NewSortedMergeIterator returns an iterator to merge itrs into one.
func NewSortedMergeIterator(inputs []Iterator, opt IteratorOptions) Iterator {
	inputs = Iterators(inputs).filterNonNil()
	if len(inputs) == 0 {
		return &nilFloatIterator{}
	}

	switch input := inputs[0].(type) {
	case FloatIterator:
		return newFloatSortedMergeIterator(newFloatIterators(inputs), opt)
	case IntegerIterator:
		return newIntegerSortedMergeIterator(newIntegerIterators(inputs), opt)
	case StringIterator:
		return newStringSortedMergeIterator(newStringIterators(inputs), opt)
	case BooleanIterator:
		return newBooleanSortedMergeIterator(newBooleanIterators(inputs), opt)
	default:
		panic(fmt.Sprintf("unsupported sorted merge iterator type: %T", input))
	}
}

// NewLimitIterator returns an iterator that limits the number of points per grouping.
func NewLimitIterator(input Iterator, opt IteratorOptions) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return newFloatLimitIterator(input, opt)
	case IntegerIterator:
		return newIntegerLimitIterator(input, opt)
	case StringIterator:
		return newStringLimitIterator(input, opt)
	case BooleanIterator:
		return newBooleanLimitIterator(input, opt)
	default:
		panic(fmt.Sprintf("unsupported limit iterator type: %T", input))
	}
}

// AuxIterator represents an iterator that can split off separate auxilary iterators.
type AuxIterator interface {
	Iterator
	IteratorCreator

	// Auxilary iterator
	Iterator(name string) Iterator
}

// NewAuxIterator returns a new instance of AuxIterator.
func NewAuxIterator(input Iterator, opt IteratorOptions) AuxIterator {
	switch input := input.(type) {
	case FloatIterator:
		return newFloatAuxIterator(input, opt)
	case IntegerIterator:
		return newIntegerAuxIterator(input, opt)
	case StringIterator:
		return newStringAuxIterator(input, opt)
	case BooleanIterator:
		return newBooleanAuxIterator(input, opt)
	default:
		panic(fmt.Sprintf("unsupported aux iterator type: %T", input))
	}
}

// auxIteratorField represents an auxilary field within an AuxIterator.
type auxIteratorField struct {
	name    string     // field name
	typ     DataType   // detected data type
	initial Point      // first point
	itrs    []Iterator // auxillary iterators
	opt     IteratorOptions
}

type auxIteratorFields []*auxIteratorField

// newAuxIteratorFields returns a new instance of auxIteratorFields from a list of field names.
func newAuxIteratorFields(opt IteratorOptions) auxIteratorFields {
	fields := make(auxIteratorFields, len(opt.Aux))
	for i, name := range opt.Aux {
		fields[i] = &auxIteratorField{name: name, opt: opt}
	}
	return fields
}

func (a auxIteratorFields) close() {
	for _, f := range a {
		for _, itr := range f.itrs {
			itr.Close()
		}
	}
}

// init initializes all auxilary fields with initial points.
func (a auxIteratorFields) init(p Point) {
	values := p.aux()
	for i, f := range a {
		v := values[i]

		tags := p.tags()
		tags = tags.Subset(f.opt.Dimensions)

		// Initialize first point based off value received.
		// Primitive pointers represent nil values.
		switch v := v.(type) {
		case float64:
			f.typ = Float
			f.initial = &FloatPoint{Name: p.name(), Tags: tags, Time: p.time(), Value: v}
		case *float64:
			f.typ = Float
			f.initial = &FloatPoint{Name: p.name(), Tags: tags, Time: p.time(), Nil: true}
		case int64:
			f.typ = Integer
			f.initial = &IntegerPoint{Name: p.name(), Tags: tags, Time: p.time(), Value: v}
		case *int64:
			f.typ = Integer
			f.initial = &IntegerPoint{Name: p.name(), Tags: tags, Time: p.time(), Nil: true}
		case string:
			f.typ = String
			f.initial = &StringPoint{Name: p.name(), Tags: tags, Time: p.time(), Value: v}
		case *string:
			f.typ = String
			f.initial = &StringPoint{Name: p.name(), Tags: tags, Time: p.time(), Nil: true}
		case bool:
			f.typ = Boolean
			f.initial = &BooleanPoint{Name: p.name(), Tags: tags, Time: p.time(), Value: v}
		case *bool:
			f.typ = Boolean
			f.initial = &BooleanPoint{Name: p.name(), Tags: tags, Time: p.time(), Nil: true}
		default:
			panic(fmt.Sprintf("invalid aux value type: %T", v))
		}
	}
}

// iterator creates a new iterator for a named auxilary field.
func (a auxIteratorFields) iterator(name string) Iterator {
	for _, f := range a {
		// Skip field if it's name doesn't match.
		// Exit if no points were received by the iterator.
		if f.name != name {
			continue
		} else if f.initial == nil {
			break
		}

		// Create channel iterator by data type.
		switch f.typ {
		case Float:
			itr := &floatChanIterator{c: make(chan *FloatPoint, 1)}
			itr.c <- f.initial.(*FloatPoint)
			f.itrs = append(f.itrs, itr)
			return itr
		case Integer:
			itr := &integerChanIterator{c: make(chan *IntegerPoint, 1)}
			itr.c <- f.initial.(*IntegerPoint)
			f.itrs = append(f.itrs, itr)
			return itr
		case String:
			itr := &stringChanIterator{c: make(chan *StringPoint, 1)}
			itr.c <- f.initial.(*StringPoint)
			f.itrs = append(f.itrs, itr)
			return itr
		case Boolean:
			itr := &booleanChanIterator{c: make(chan *BooleanPoint, 1)}
			itr.c <- f.initial.(*BooleanPoint)
			f.itrs = append(f.itrs, itr)
			return itr
		default:
			panic(fmt.Sprintf("unsupported chan iterator type: %s", f.typ))
		}
	}

	return &nilFloatIterator{}
}

// send sends a point to all field iterators.
func (a auxIteratorFields) send(p Point) {
	values := p.aux()
	for i, f := range a {
		v := values[i]

		tags := p.tags()
		tags = tags.Subset(f.opt.Dimensions)

		// Send new point for each aux iterator.
		// Primitive pointers represent nil values.
		for _, itr := range f.itrs {
			switch itr := itr.(type) {
			case *floatChanIterator:
				switch v := v.(type) {
				case float64:
					itr.c <- &FloatPoint{Name: p.name(), Tags: tags, Time: p.time(), Value: v}
				default:
					itr.c <- &FloatPoint{Name: p.name(), Tags: tags, Time: p.time(), Nil: true}
				}
			case *integerChanIterator:
				switch v := v.(type) {
				case int64:
					itr.c <- &IntegerPoint{Name: p.name(), Tags: tags, Time: p.time(), Value: v}
				default:
					itr.c <- &IntegerPoint{Name: p.name(), Tags: tags, Time: p.time(), Nil: true}
				}
			case *stringChanIterator:
				switch v := v.(type) {
				case string:
					itr.c <- &StringPoint{Name: p.name(), Tags: tags, Time: p.time(), Value: v}
				default:
					itr.c <- &StringPoint{Name: p.name(), Tags: tags, Time: p.time(), Nil: true}
				}
			case *booleanChanIterator:
				switch v := v.(type) {
				case bool:
					itr.c <- &BooleanPoint{Name: p.name(), Tags: tags, Time: p.time(), Value: v}
				default:
					itr.c <- &BooleanPoint{Name: p.name(), Tags: tags, Time: p.time(), Nil: true}
				}
			default:
				panic(fmt.Sprintf("invalid aux itr type: %T", itr))
			}
		}
	}
}

// drainIterator reads all points from an iterator.
func drainIterator(itr Iterator) {
	for {
		switch itr := itr.(type) {
		case FloatIterator:
			if p := itr.Next(); p == nil {
				return
			}
		case IntegerIterator:
			if p := itr.Next(); p == nil {
				return
			}
		case StringIterator:
			if p := itr.Next(); p == nil {
				return
			}
		case BooleanIterator:
			if p := itr.Next(); p == nil {
				return
			}
		default:
			panic(fmt.Sprintf("unsupported iterator type for draining: %T", itr))
		}
	}
}

// IteratorCreator represents an interface for objects that can create Iterators.
type IteratorCreator interface {
	// Creates a simple iterator for use in an InfluxQL query.
	CreateIterator(opt IteratorOptions) (Iterator, error)

	// Returns the unique fields and dimensions across a list of sources.
	FieldDimensions(sources Sources) (fields, dimensions map[string]struct{}, err error)
}

// IteratorOptions is an object passed to CreateIterator to specify creation options.
type IteratorOptions struct {
	// Expression to iterate for.
	// This can be VarRef or a Call.
	Expr Expr

	// Auxilary tags or values to also retrieve for the point.
	Aux []string

	// Data sources from which to retrieve data.
	Sources []Source

	// Group by interval and tags.
	Interval   Interval
	Dimensions []string

	// Condition to filter by.
	Condition Expr

	// Time range for the iterator.
	StartTime int64
	EndTime   int64

	// Sorted in time ascending order if true.
	Ascending bool

	// Limits the number of points per series.
	Limit, Offset int

	// Limits the number of series.
	SLimit, SOffset int
}

// newIteratorOptionsStmt creates the iterator options from stmt.
func newIteratorOptionsStmt(stmt *SelectStatement) (opt IteratorOptions, err error) {
	// Determine time range from the condition.
	startTime, endTime := TimeRange(stmt.Condition)
	if !startTime.IsZero() {
		opt.StartTime = startTime.UnixNano()
	} else {
		opt.StartTime = MinTime
	}
	if !endTime.IsZero() {
		opt.EndTime = endTime.UnixNano()
	} else {
		opt.EndTime = MaxTime
	}

	// Determine group by interval.
	interval, err := stmt.GroupByInterval()
	if err != nil {
		return opt, err
	}
	// Set duration to zero if a negative interval has been used.
	if interval < 0 {
		interval = 0
	}
	opt.Interval.Duration = interval

	// Determine dimensions.
	for _, d := range stmt.Dimensions {
		if d, ok := d.Expr.(*VarRef); ok {
			opt.Dimensions = append(opt.Dimensions, d.Val)
		}
	}

	opt.Sources = stmt.Sources
	opt.Condition = stmt.Condition
	opt.Ascending = stmt.TimeAscending()

	opt.Limit, opt.Offset = stmt.Limit, stmt.Offset
	opt.SLimit, opt.SOffset = stmt.SLimit, stmt.SOffset

	return opt, nil
}

// MergeSorted returns true if the options require a sorted merge.
// This is only needed when the expression is a variable reference or there is no expr.
func (opt IteratorOptions) MergeSorted() bool {
	if opt.Expr == nil {
		return true
	}
	_, ok := opt.Expr.(*VarRef)
	return ok
}

// SeekTime returns the time the iterator should start from.
// For ascending iterators this is the start time, for descending iterators it's the end time.
func (opt IteratorOptions) SeekTime() int64 {
	if opt.Ascending {
		return opt.StartTime
	}
	return opt.EndTime
}

// Window returns the time window [start,end) that t falls within.
func (opt IteratorOptions) Window(t int64) (start, end int64) {
	if opt.Interval.IsZero() {
		return opt.StartTime, opt.EndTime
	}

	// Truncate time by duration.
	t -= t % int64(opt.Interval.Duration)

	start = t + int64(opt.Interval.Offset)
	end = start + int64(opt.Interval.Duration)
	return
}

// DerivativeInterval returns the time interval for the derivative function.
func (opt IteratorOptions) DerivativeInterval() Interval {
	// Use the interval on the derivative() call, if specified.
	if expr, ok := opt.Expr.(*Call); ok && len(expr.Args) == 2 {
		return Interval{Duration: expr.Args[1].(*DurationLiteral).Val}
	}

	// Otherwise use the group by interval, if specified.
	if opt.Interval.Duration > 0 {
		return opt.Interval
	}

	return Interval{Duration: time.Second}
}

// selectInfo represents an object that stores info about select fields.
type selectInfo struct {
	calls map[*Call]struct{}
	refs  map[*VarRef]struct{}
}

// newSelectInfo creates a object with call and var ref info from stmt.
func newSelectInfo(stmt *SelectStatement) *selectInfo {
	info := &selectInfo{
		calls: make(map[*Call]struct{}),
		refs:  make(map[*VarRef]struct{}),
	}
	Walk(info, stmt.Fields)
	return info
}

func (v *selectInfo) Visit(n Node) Visitor {
	switch n := n.(type) {
	case *Call:
		v.calls[n] = struct{}{}
		return nil
	case *VarRef:
		v.refs[n] = struct{}{}
		return nil
	}
	return v
}

// Interval represents a repeating interval for a query.
type Interval struct {
	Duration time.Duration
	Offset   time.Duration
}

// IsZero returns true if the interval has no duration.
func (i Interval) IsZero() bool { return i.Duration == 0 }

// reduceOptions represents options for performing reductions on windows of points.
type reduceOptions struct {
	startTime int64
	endTime   int64
}

type nilFloatIterator struct{}

func (*nilFloatIterator) Close() error      { return nil }
func (*nilFloatIterator) Next() *FloatPoint { return nil }

// integerReduceSliceFloatIterator executes a reducer on all points in a window and buffers the result.
// This iterator receives an integer iterator but produces a float iterator.
type integerReduceSliceFloatIterator struct {
	input  *bufIntegerIterator
	fn     integerReduceSliceFloatFunc
	opt    IteratorOptions
	points []FloatPoint
}

// Close closes the iterator and all child iterators.
func (itr *integerReduceSliceFloatIterator) Close() error { return itr.input.Close() }

// Next returns the minimum value for the next available interval.
func (itr *integerReduceSliceFloatIterator) Next() *FloatPoint {
	// Calculate next window if we have no more points.
	if len(itr.points) == 0 {
		itr.points = itr.reduce()
		if len(itr.points) == 0 {
			return nil
		}
	}

	// Pop next point off the stack.
	p := itr.points[len(itr.points)-1]
	itr.points = itr.points[:len(itr.points)-1]
	return &p
}

// reduce executes fn once for every point in the next window.
// The previous value for the dimension is passed to fn.
func (itr *integerReduceSliceFloatIterator) reduce() []FloatPoint {
	// Calculate next window.
	startTime, endTime := itr.opt.Window(itr.input.peekTime())

	var reduceOptions = reduceOptions{
		startTime: startTime,
		endTime:   endTime,
	}

	// Group points by name and tagset.
	groups := make(map[string]struct {
		name   string
		tags   Tags
		points []IntegerPoint
	})
	for {
		// Read next point.
		p := itr.input.NextInWindow(startTime, endTime)
		if p == nil {
			break
		}
		tags := p.Tags.Subset(itr.opt.Dimensions)

		// Append point to dimension.
		id := tags.ID()
		g := groups[id]
		g.name = p.Name
		g.tags = tags
		g.points = append(g.points, *p)
		groups[id] = g
	}

	// Reduce each set into a set of values.
	results := make(map[string][]FloatPoint)
	for key, g := range groups {
		a := itr.fn(g.points, &reduceOptions)
		if len(a) == 0 {
			continue
		}

		// Update name and tags for each returned point.
		for i := range a {
			a[i].Name = g.name
			a[i].Tags = g.tags
		}
		results[key] = a
	}

	// Reverse sort points by name & tag.
	keys := make([]string, 0, len(results))
	for k := range results {
		keys = append(keys, k)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(keys)))

	// Reverse order points within each key.
	a := make([]FloatPoint, 0, len(results))
	for _, k := range keys {
		for i := len(results[k]) - 1; i >= 0; i-- {
			a = append(a, results[k][i])
		}
	}

	return a
}

// integerReduceSliceFloatFunc is the function called by a IntegerPoint slice reducer that emits FloatPoint.
type integerReduceSliceFloatFunc func(a []IntegerPoint, opt *reduceOptions) []FloatPoint

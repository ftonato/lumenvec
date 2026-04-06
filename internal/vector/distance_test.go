package vector

import (
	"math"
	"testing"
)

func TestEuclideanDistance(t *testing.T) {
	got := EuclideanDistance([]float64{1, 2}, []float64{1, 5})
	if got != 3 {
		t.Fatalf("EuclideanDistance() = %v", got)
	}
}

func TestEuclideanDistanceDimensionMismatch(t *testing.T) {
	if !math.IsNaN(EuclideanDistance([]float64{1}, []float64{1, 2})) {
		t.Fatal("expected NaN")
	}
}

func TestCosineDistance(t *testing.T) {
	got := CosineDistance([]float64{1, 0}, []float64{1, 0})
	if got != 0 {
		t.Fatalf("CosineDistance() = %v", got)
	}
}

func TestCosineDistanceZeroVectorAndMismatch(t *testing.T) {
	if got := CosineDistance([]float64{0, 0}, []float64{1, 0}); got != 1 {
		t.Fatalf("CosineDistance() zero = %v", got)
	}
	if !math.IsNaN(CosineDistance([]float64{1}, []float64{1, 2})) {
		t.Fatal("expected NaN")
	}
}

func TestVectorOperations(t *testing.T) {
	v1 := NewVector([]float64{3, 4})
	v2 := NewVector([]float64{1, 2})

	added := v1.Add(v2)
	if added == nil || added.Values[0] != 4 || added.Values[1] != 6 {
		t.Fatal("unexpected Add result")
	}

	sub := v1.Subtract(v2)
	if sub == nil || sub.Values[0] != 2 || sub.Values[1] != 2 {
		t.Fatal("unexpected Subtract result")
	}

	norm := v1.Normalize()
	if norm == nil || math.Abs(norm.Length()-1) > 1e-9 {
		t.Fatal("expected normalized vector")
	}

	if v1.Length() != 5 {
		t.Fatalf("Length() = %v", v1.Length())
	}
}

func TestVectorOperationsMismatchAndZeroNormalize(t *testing.T) {
	if NewVector([]float64{1}).Add(NewVector([]float64{1, 2})) != nil {
		t.Fatal("expected nil for mismatched Add")
	}
	if NewVector([]float64{1}).Subtract(NewVector([]float64{1, 2})) != nil {
		t.Fatal("expected nil for mismatched Subtract")
	}
	if NewVector([]float64{0, 0}).Normalize() != nil {
		t.Fatal("expected nil for zero Normalize")
	}
}

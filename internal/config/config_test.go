package config

import (
	"reflect"
	"testing"
	"time"
)

func TestString(t *testing.T) {
	t.Setenv("KL_TEST_STR", "hello")
	if got := String("KL_TEST_STR", "def"); got != "hello" {
		t.Fatalf("set: got %q want %q", got, "hello")
	}
	if got := String("KL_TEST_ABSENT", "def"); got != "def" {
		t.Fatalf("absent: got %q want %q", got, "def")
	}
}

// An unset compose variable interpolates to the EMPTY STRING rather than
// vanishing, so empty must be treated as unset or every default in
// docker-compose.yml would be silently overridden by "".
func TestStringEmptyAndWhitespaceFallBackToDefault(t *testing.T) {
	t.Setenv("KL_TEST_EMPTY", "")
	if got := String("KL_TEST_EMPTY", "def"); got != "def" {
		t.Fatalf("empty: got %q want %q", got, "def")
	}
	t.Setenv("KL_TEST_WS", "   ")
	if got := String("KL_TEST_WS", "def"); got != "def" {
		t.Fatalf("whitespace: got %q want %q", got, "def")
	}
}

func TestStringTrims(t *testing.T) {
	t.Setenv("KL_TEST_TRIM", "  padded  ")
	if got := String("KL_TEST_TRIM", "def"); got != "padded" {
		t.Fatalf("got %q want %q", got, "padded")
	}
}

func TestInt(t *testing.T) {
	t.Setenv("KL_TEST_INT", "42")
	if got := Int("KL_TEST_INT", 7); got != 42 {
		t.Fatalf("set: got %d want 42", got)
	}
	if got := Int("KL_TEST_INT_ABSENT", 7); got != 7 {
		t.Fatalf("absent: got %d want 7", got)
	}
	t.Setenv("KL_TEST_INT_BAD", "not-a-number")
	if got := Int("KL_TEST_INT_BAD", 7); got != 7 {
		t.Fatalf("unparseable: got %d want 7", got)
	}
}

func TestFloat(t *testing.T) {
	t.Setenv("KL_TEST_F", "2.5")
	if got := Float("KL_TEST_F", 1); got != 2.5 {
		t.Fatalf("set: got %v want 2.5", got)
	}
	if got := Float("KL_TEST_F_ABSENT", 1); got != 1 {
		t.Fatalf("absent: got %v want 1", got)
	}
	t.Setenv("KL_TEST_F_BAD", "x")
	if got := Float("KL_TEST_F_BAD", 1); got != 1 {
		t.Fatalf("unparseable: got %v want 1", got)
	}
}

func TestDuration(t *testing.T) {
	t.Setenv("KL_TEST_D", "1500ms")
	if got := Duration("KL_TEST_D", time.Second); got != 1500*time.Millisecond {
		t.Fatalf("set: got %v", got)
	}
	if got := Duration("KL_TEST_D_ABSENT", time.Second); got != time.Second {
		t.Fatalf("absent: got %v", got)
	}
	t.Setenv("KL_TEST_D_BAD", "nope")
	if got := Duration("KL_TEST_D_BAD", time.Second); got != time.Second {
		t.Fatalf("unparseable: got %v", got)
	}
}

func TestBool(t *testing.T) {
	t.Setenv("KL_TEST_B", "true")
	if !Bool("KL_TEST_B", false) {
		t.Fatal("set: want true")
	}
	if Bool("KL_TEST_B_ABSENT", false) {
		t.Fatal("absent: want false")
	}
	t.Setenv("KL_TEST_B_BAD", "maybe")
	if Bool("KL_TEST_B_BAD", false) {
		t.Fatal("unparseable: want false")
	}
}

func TestBrokers(t *testing.T) {
	t.Setenv("KL_TEST_BROKERS", " a:1 , b:2 ,, ")
	want := []string{"a:1", "b:2"}
	if got := Brokers("KL_TEST_BROKERS", "z:9"); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if got := Brokers("KL_TEST_BROKERS_ABSENT", "z:9"); !reflect.DeepEqual(got, []string{"z:9"}) {
		t.Fatalf("absent: got %v", got)
	}
	if got := Brokers("KL_TEST_BROKERS_ABSENT", ""); len(got) != 0 {
		t.Fatalf("empty default: got %v want empty", got)
	}
}

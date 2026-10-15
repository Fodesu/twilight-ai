package prompt

import "testing"

func TestTokenBudgetExceeded(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	tests := []struct {
		name     string
		budget   TokenBudget
		input    int
		exceeded bool
		invalid  bool
	}{
		{"window exact", TokenBudget{ContextWindow: 100}, 100, false, false},
		{"window exceeded", TokenBudget{ContextWindow: 100}, 101, true, false},
		{"both reserves exact", TokenBudget{ContextWindow: 100, OutputReserve: 20, SafetyMargin: 10}, 70, false, false},
		{"both reserves exceeded", TokenBudget{ContextWindow: 100, OutputReserve: 20, SafetyMargin: 10}, 71, true, false},
		{"input cap exact", TokenBudget{ContextWindow: 100, InputLimit: 40}, 40, false, false},
		{"input cap exceeded", TokenBudget{ContextWindow: 100, InputLimit: 40}, 41, true, false},
		{"cap cannot enlarge", TokenBudget{ContextWindow: 100, OutputReserve: 20, SafetyMargin: 10, InputLimit: 200}, 71, true, false},
		{"zero capacity empty", TokenBudget{ContextWindow: 100, OutputReserve: 70, SafetyMargin: 30}, 0, false, false},
		{"zero capacity nonempty", TokenBudget{ContextWindow: 100, OutputReserve: 70, SafetyMargin: 30}, 1, true, false},
		{"zero window", TokenBudget{}, 0, false, true},
		{"negative window", TokenBudget{ContextWindow: -1}, 0, false, true},
		{"negative reserve", TokenBudget{ContextWindow: 100, OutputReserve: -1}, 0, false, true},
		{"negative margin", TokenBudget{ContextWindow: 100, SafetyMargin: -1}, 0, false, true},
		{"negative cap", TokenBudget{ContextWindow: 100, InputLimit: -1}, 0, false, true},
		{"negative input", TokenBudget{ContextWindow: 100}, -1, false, true},
		{"reserve too large", TokenBudget{ContextWindow: 100, OutputReserve: 101}, 0, false, true},
		{"margin too large", TokenBudget{ContextWindow: 100, SafetyMargin: 101}, 0, false, true},
		{"combined reserves too large", TokenBudget{ContextWindow: 100, OutputReserve: 60, SafetyMargin: 41}, 0, false, true},
		{"max input exact", TokenBudget{ContextWindow: maxInt}, maxInt, false, false},
		{"max input with reserves", TokenBudget{ContextWindow: maxInt, OutputReserve: 1, SafetyMargin: 1}, maxInt, true, false},
		{"max reserve exact", TokenBudget{ContextWindow: maxInt, OutputReserve: maxInt}, 0, false, false},
		{"max margin exact", TokenBudget{ContextWindow: maxInt, SafetyMargin: maxInt}, 0, false, false},
		{"reserve sum overflow", TokenBudget{ContextWindow: maxInt, OutputReserve: maxInt, SafetyMargin: maxInt}, 0, false, true},
		{"reserve plus one overflow", TokenBudget{ContextWindow: maxInt, OutputReserve: maxInt, SafetyMargin: 1}, 0, false, true},
		{"max cap cannot enlarge", TokenBudget{ContextWindow: 1, InputLimit: maxInt}, maxInt, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.budget.Exceeded(tt.input)
			if (err != nil) != tt.invalid || got != tt.exceeded {
				t.Fatalf("Exceeded(%d) = %v, %v; want exceeded=%v invalid=%v", tt.input, got, err, tt.exceeded, tt.invalid)
			}
		})
	}
}

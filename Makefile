CXX ?= c++
AR ?= ar
BUILD ?= build
CXXFLAGS ?= -O2 -g
CPPFLAGS += -I. -D_GNU_SOURCE
WARNINGS := -Wall -Wextra -Wpedantic -Wconversion -Wshadow
CPP_DIRS := graph kernel mailbox barrier checkpoint workers
CORE_CPP := $(wildcard $(addsuffix /*.cpp,$(CPP_DIRS)))
CORE_OBJ := $(patsubst %.cpp,$(BUILD)/%.o,$(CORE_CPP))
CLI_OBJ := $(BUILD)/app/main.o $(BUILD)/app/inspection.o
BENCH_OBJ := $(BUILD)/app/bench.o
TEST_OBJ := $(BUILD)/tests/main.o
DEPENDENCIES := $(CORE_OBJ:.o=.d) $(CLI_OBJ:.o=.d) $(BENCH_OBJ:.o=.d) $(TEST_OBJ:.o=.d)
SUITES := graph kernel exchange snapshot engine

.PHONY: all check demo bench
all: $(BUILD)/frontierloom $(BUILD)/frontier-bench

$(BUILD)/%.o: %.cpp
	@mkdir -p $(@D)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(WARNINGS) -std=c++20 -MMD -MP -c $< -o $@

$(BUILD)/libfrontier_core.a: $(CORE_OBJ)
	$(AR) rcs $@ $^

$(BUILD)/frontierloom: $(CLI_OBJ) $(BUILD)/libfrontier_core.a
	$(CXX) $(LDFLAGS) $^ $(LDLIBS) -o $@

$(BUILD)/frontier-bench: $(BENCH_OBJ) $(BUILD)/libfrontier_core.a
	$(CXX) $(LDFLAGS) $^ $(LDLIBS) -o $@

$(BUILD)/frontier-tests: $(TEST_OBJ) $(BUILD)/libfrontier_core.a
	$(CXX) $(LDFLAGS) $^ $(LDLIBS) -o $@

check: $(BUILD)/frontier-tests
	@set -e; for suite in $(SUITES); do $(BUILD)/frontier-tests $$suite samples; done

demo: $(BUILD)/frontierloom
	bash scripts/demo.sh $(BUILD)

bench: $(BUILD)/frontier-bench
	bash scripts/bench.sh $(BUILD)

-include $(DEPENDENCIES)

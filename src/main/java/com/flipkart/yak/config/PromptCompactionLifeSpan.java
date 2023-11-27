package com.flipkart.yak.config;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Data
@SuperBuilder
@RequiredArgsConstructor
@Jacksonized
public class PromptCompactionLifeSpan {
    final long startSpan;
    final long endSpan;
}

//! Post-parse validation. Aggregates every rule defect into a single
//! `PolicyError::Validate` so a user with five typos sees them at once.

use crate::{
    config::{DenyRule, Policy},
    error::PolicyError,
};

pub(crate) fn validate(policy: &Policy) -> Result<(), PolicyError> {
    let mut errors = Vec::new();

    for (i, rule) in policy.shell.allow.iter().enumerate() {
        check_cmd(&format!("shell.allow[{i}].cmd"), &rule.cmd, &mut errors);
        for (j, sub) in rule.subcmds.iter().enumerate() {
            check_cmd(&format!("shell.allow[{i}].subcmds[{j}]"), sub, &mut errors);
        }
    }

    for (i, rule) in policy.shell.deny.iter().enumerate() {
        match rule {
            DenyRule::Cmd(d) => {
                check_cmd(&format!("shell.deny[{i}].cmd"), &d.cmd, &mut errors);
            }
            DenyRule::Pattern(p) => {
                if p.pattern.is_empty() {
                    errors.push(format!("shell.deny[{i}].pattern is empty"));
                }
            }
        }
    }

    if let Some(llm) = policy.llm.as_ref() {
        check_llm_rules("llm.allow", &llm.allow, &mut errors);
        check_llm_rules("llm.deny", &llm.deny, &mut errors);
    }

    check_mcp_rules("mcp.allow", &policy.mcp.allow, &mut errors);
    check_mcp_rules("mcp.deny", &policy.mcp.deny, &mut errors);

    // 5.8 (Codex P2 on #59): monetary limits must be finite and
    // non-negative — TOML happily parses `nan`/`inf`, and a NaN cap
    // makes every `spent > cap` comparison false, silently disabling
    // the budget; a negative cap trips on the first $0 step.
    check_usd(
        "execution.default_plan_budget_usd",
        policy.execution.default_plan_budget_usd,
        &mut errors,
    );
    check_usd(
        "execution.plan_budget_ceiling_usd",
        policy.execution.plan_budget_ceiling_usd,
        &mut errors,
    );

    // 5.9: pricing overrides — non-empty prefixes, finite non-negative
    // rates (same nan/negative hygiene as the budget knobs).
    for (prefix, rates) in &policy.cost.model_prices {
        if prefix.trim().is_empty() {
            errors.push("cost.model_prices has an empty model prefix".to_string());
        }
        for (i, r) in rates.iter().enumerate() {
            if !r.is_finite() || *r < 0.0 {
                errors.push(format!(
                    "cost.model_prices[{prefix:?}][{i}] must be a finite, non-negative rate (got {r})"
                ));
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(PolicyError::Validate { errors })
    }
}

fn check_llm_rules(prefix: &str, rules: &[crate::config::LlmAllow], errors: &mut Vec<String>) {
    for (i, rule) in rules.iter().enumerate() {
        match rule {
            crate::config::LlmAllow::Model(m) => {
                check_llm_field(&format!("{prefix}[{i}].model"), &m.model, errors);
            }
            crate::config::LlmAllow::Prefix(p) => {
                check_llm_field(
                    &format!("{prefix}[{i}].model_prefix"),
                    &p.model_prefix,
                    errors,
                );
            }
        }
    }
}

fn check_mcp_rules(prefix: &str, rules: &[crate::config::McpRule], errors: &mut Vec<String>) {
    for (i, rule) in rules.iter().enumerate() {
        check_llm_field(&format!("{prefix}[{i}].server"), &rule.server, errors);
        if let Some(tool) = rule.tool.as_deref() {
            check_llm_field(&format!("{prefix}[{i}].tool"), tool, errors);
        }
    }
}

fn check_llm_field(field: &str, value: &str, errors: &mut Vec<String>) {
    if value.is_empty() {
        errors.push(format!("{field} is empty"));
        return;
    }
    if value.chars().any(char::is_whitespace) {
        errors.push(format!("{field} contains whitespace ({value:?})"));
    }
}

fn check_cmd(field: &str, value: &str, errors: &mut Vec<String>) {
    if value.is_empty() {
        errors.push(format!("{field} is empty"));
        return;
    }
    if value.chars().any(char::is_whitespace) {
        errors.push(format!(
            "{field} contains whitespace ({value:?}); split into cmd + subcmds"
        ));
    }
}

fn check_usd(field: &str, value: Option<f64>, errors: &mut Vec<String>) {
    if let Some(v) = value {
        if !v.is_finite() || v < 0.0 {
            errors.push(format!(
                "{field} must be a finite, non-negative dollar amount (got {v})"
            ));
        }
    }
}

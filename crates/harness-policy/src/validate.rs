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

use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

const MAX_OUTPUT_BYTES: usize = 128;
const VARIABLES: [&str; 11] = [
    "trigger",
    "indexer_id",
    "indexer_name",
    "indexer_slug",
    "match_mode",
    "source_category",
    "source_kind",
    "video_kind",
    "year",
    "season",
    "episode",
];

#[derive(Debug, Default)]
pub(crate) struct TemplateContext {
    values: BTreeMap<&'static str, String>,
}

impl TemplateContext {
    pub(crate) fn insert(&mut self, name: &'static str, value: impl Into<String>) {
        debug_assert!(VARIABLES.contains(&name));
        self.values.insert(name, value.into());
    }

    pub(crate) fn render_category(&self, template: &str) -> Result<String, TemplateError> {
        sanitize(&render(template, &self.values)?, false)
    }

    pub(crate) fn render_tags(
        &self,
        templates: &[String],
        inherited: impl IntoIterator<Item = String>,
    ) -> Result<Vec<String>, TemplateError> {
        let mut tags = Vec::new();
        let mut seen = BTreeSet::new();
        for value in templates
            .iter()
            .map(|template| render(template, &self.values))
            .chain(inherited.into_iter().map(Ok))
        {
            let tag = sanitize(&value?, true)?;
            if !tag.is_empty() && seen.insert(tag.clone()) {
                tags.push(tag);
            }
        }
        Ok(tags)
    }
}

pub(crate) fn validate(template: &str) -> Result<(), TemplateError> {
    let values = VARIABLES
        .into_iter()
        .map(|name| (name, String::new()))
        .collect();
    render(template, &values).map(|_| ())
}

pub(crate) fn slug(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut separator = false;
    for character in value.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_alphanumeric() {
            if separator && !result.is_empty() {
                result.push('-');
            }
            result.push(character);
            separator = false;
        } else {
            separator = true;
        }
    }
    result
}

fn render(
    template: &str,
    values: &BTreeMap<&'static str, String>,
) -> Result<String, TemplateError> {
    if template.contains("{%") || template.contains("{#") {
        return Err(TemplateError::UnsupportedSyntax);
    }
    let mut output = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        let literal = &rest[..start];
        if literal.contains("}}") {
            return Err(TemplateError::UnsupportedSyntax);
        }
        output.push_str(literal);
        rest = &rest[start + 2..];
        let end = rest.find("}}").ok_or(TemplateError::UnsupportedSyntax)?;
        let name = rest[..end].trim();
        if name.is_empty() || !VARIABLES.contains(&name) {
            return Err(TemplateError::UnknownVariable(name.to_owned()));
        }
        output.push_str(
            values
                .get(name)
                .ok_or_else(|| TemplateError::UndefinedVariable(name.to_owned()))?,
        );
        rest = &rest[end + 2..];
    }
    if rest.contains("}}") {
        return Err(TemplateError::UnsupportedSyntax);
    }
    output.push_str(rest);
    Ok(output)
}

fn sanitize(value: &str, tag: bool) -> Result<String, TemplateError> {
    let mut output = String::with_capacity(value.len().min(MAX_OUTPUT_BYTES));
    for character in value.trim().chars() {
        if character.is_control() || (tag && character == ',') {
            continue;
        }
        if output.len().saturating_add(character.len_utf8()) > MAX_OUTPUT_BYTES {
            break;
        }
        output.push(character);
    }
    let output = output.trim().to_owned();
    if output.len() > MAX_OUTPUT_BYTES {
        return Err(TemplateError::OutputLimit);
    }
    Ok(output)
}

#[derive(Debug, Error)]
pub(crate) enum TemplateError {
    #[error("template uses unsupported syntax")]
    UnsupportedSyntax,
    #[error("template variable {0:?} is not allowed")]
    UnknownVariable(String),
    #[error("template variable {0:?} is undefined")]
    UndefinedVariable(String),
    #[error("rendered template exceeds its output limit")]
    OutputLimit,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_only_declared_variables() {
        let mut context = TemplateContext::default();
        for variable in VARIABLES {
            context.insert(variable, "");
        }
        context.insert("trigger", "autobrr");
        context.insert("indexer_slug", "example-indexer");

        assert_eq!(
            context
                .render_category("{{ trigger }}/{{ indexer_slug }}")
                .unwrap(),
            "autobrr/example-indexer"
        );
        assert!(matches!(
            context.render_category("{{ now }}"),
            Err(TemplateError::UnknownVariable(_))
        ));
        assert!(matches!(
            context.render_category("{% include 'secret' %}"),
            Err(TemplateError::UnsupportedSyntax)
        ));
    }

    #[test]
    fn sanitizes_and_deduplicates_tags_in_order() {
        let context = TemplateContext::default();
        let tags = context
            .render_tags(
                &["sporos".to_owned(), "sporos".to_owned()],
                ["bad,tag\n".to_owned(), String::new()],
            )
            .unwrap();

        assert_eq!(tags, ["sporos", "badtag"]);
    }

    #[test]
    fn slug_is_stable_ascii() {
        assert_eq!(slug("  Example / Indexer 2 "), "example-indexer-2");
    }
}

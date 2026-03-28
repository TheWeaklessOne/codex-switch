use std::collections::BTreeSet;

use crate::domain::identity::IdentityId;
use crate::error::Result;

const MYTHIC_AUTO_NAMES: [&str; 12] = [
    "Atlas", "Vesper", "Ember", "Oracle", "Rune", "Solstice", "Sable", "Phoenix", "Aether", "Halo",
    "Fable", "Onyx",
];

pub fn next_auto_display_name<'a, I>(existing_ids: I) -> Result<String>
where
    I: IntoIterator<Item = &'a IdentityId>,
{
    let used_ids = existing_ids
        .into_iter()
        .map(|identity_id| identity_id.as_str().to_string())
        .collect::<BTreeSet<_>>();

    for round in 0usize.. {
        for base_name in MYTHIC_AUTO_NAMES {
            let candidate = if round == 0 {
                base_name.to_string()
            } else {
                format!("{base_name} {}", round + 1)
            };
            let candidate_id = IdentityId::from_display_name(&candidate)?;
            if !used_ids.contains(candidate_id.as_str()) {
                return Ok(candidate);
            }
        }
    }

    unreachable!("mythic auto-name stream is infinite");
}

#[cfg(test)]
mod tests {
    use crate::domain::identity::IdentityId;

    use super::next_auto_display_name;

    #[test]
    fn chooses_atlas_for_the_first_generated_name() {
        assert_eq!(next_auto_display_name([].iter()).unwrap(), "Atlas");
    }

    #[test]
    fn skips_taken_candidates_by_slug() {
        let atlas = IdentityId::from_display_name("Atlas").unwrap();
        let vesper = IdentityId::from_display_name("Vesper").unwrap();
        assert_eq!(next_auto_display_name([&atlas, &vesper]).unwrap(), "Ember");
    }

    #[test]
    fn rolls_over_to_numbered_suffix_after_pool_exhaustion() {
        let used = [
            "Atlas", "Vesper", "Ember", "Oracle", "Rune", "Solstice", "Sable", "Phoenix", "Aether",
            "Halo", "Fable", "Onyx",
        ]
        .into_iter()
        .map(|name| IdentityId::from_display_name(name).unwrap())
        .collect::<Vec<_>>();

        assert_eq!(next_auto_display_name(used.iter()).unwrap(), "Atlas 2");
    }
}

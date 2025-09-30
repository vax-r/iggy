pub mod continuous;
pub mod oneshot;
pub mod periodic;

use super::registry::TaskRegistry;

// Marker type for when no shutdown callback is provided
pub struct NoShutdown;

impl TaskRegistry {
    pub fn periodic(&self, name: &'static str) -> periodic::PeriodicBuilder<'_, (), NoShutdown> {
        periodic::PeriodicBuilder::new(self, name)
    }

    pub fn continuous(
        &self,
        name: &'static str,
    ) -> continuous::ContinuousBuilder<'_, (), NoShutdown> {
        continuous::ContinuousBuilder::new(self, name)
    }

    pub fn oneshot(&self, name: &'static str) -> oneshot::OneShotBuilder<'_, (), NoShutdown> {
        oneshot::OneShotBuilder::new(self, name)
    }
}

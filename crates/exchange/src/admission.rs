//! The admission a pair launcher hands both processes of a Nexus, checked.

use crate::generated::Admission;
use crate::lane::HEADER_BYTES;

/// The bootstrap admission as the processes use it: the deadline every frame
/// is read and written within, the largest frame admitted, and how long a
/// transient accept failure waits before the next accept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidAdmission {
    pub deadline: std::time::Duration,
    pub frame_capacity: usize,
    pub retry_delay: std::time::Duration,
}

impl TryFrom<&Admission> for ValidAdmission {
    type Error = &'static str;

    fn try_from(value: &Admission) -> Result<Self, Self::Error> {
        use shared_translator::unix::AdmissionAccess;
        let milliseconds = u64::try_from(value.frame_deadline_milliseconds.0)
            .ok()
            .filter(|value| *value > 0)
            .ok_or("frame deadline must be positive and representable")?;
        let deadline = std::time::Duration::from_millis(milliseconds);
        let frame_capacity = usize::try_from(value.frame_capacity_bytes.0)
            .ok()
            .filter(|value| (HEADER_BYTES..=signal::MAXIMUM_SIGNAL_BYTES).contains(value))
            .ok_or("frame capacity is outside the shared wire bound")?;
        let retry_milliseconds = u64::try_from(value.retry_delay_milliseconds.0)
            .ok()
            .filter(|value| *value > 0)
            .ok_or("retry delay must be positive and representable")?;
        let retry_delay = std::time::Duration::from_millis(retry_milliseconds);
        let admission =
            shared_translator::unix::Admission::new(deadline, frame_capacity, retry_delay)?;
        Ok(Self {
            deadline: admission.deadline(),
            frame_capacity: admission.frame_capacity(),
            retry_delay: admission.retry_delay(),
        })
    }
}

//! Model Predictive Control (MPC) commit pipeline controller (IMPL-26).
//!
//! Scaffolding-only — this controller is not wired into real commit paths yet.
//! The intent is to pin the API and algebra so downstream beads can replace the
//! heuristic in one place.
//!
//! The dynamics are a simple linearised single-dimensional queue model:
//!
//! ```text
//!     x_{k+1} = a * x_k + b * u_k + w_k
//! ```
//!
//! where `x` is observed queue depth (pending commits), `u` is admission rate
//! (commits per tick we are willing to accept), and `w` is an unmodelled
//! disturbance. `a` captures natural drain (e.g. replication, flush), and `b`
//! is the actuator gain — i.e. how strongly admission rate moves the queue.
//!
//! For a one-step horizon LQR with cost `(x_{k+1} - target)^2 + rho * (u - u_prev)^2`
//! the unconstrained optimum is closed-form:
//!
//! ```text
//!     u* = (target - a * x) / b
//! ```
//!
//! (Derived from requiring `a*x + b*u = target`; note admission *raises* the
//! queue so u is positive when the queue is below target and negative when
//! above; the `[0, max_rate]` clamp handles the "above target → don't admit,
//! let natural drain `a` do the work" case.)
//!
//! After clamping into `[0, max_rate]` we smooth toward the previous control to
//! avoid chattering:
//!
//! ```text
//!     u = u_prev + (1 / (1 + rho)) * (u* - u_prev)
//! ```
//!
//! This is intentionally 20-ish lines of real logic. Replace with a multi-step
//! horizon QP when we start caring about anticipated disturbance profiles.

/// Default control-effort weight. Higher values damp adjustments harder.
const DEFAULT_RHO: f64 = 1.0;

/// Minimum positive actuator gain. Protects against division-by-zero when
/// a caller accidentally plugs in `b = 0`.
const MIN_B: f64 = 1e-9;

/// One-step MPC controller for the commit admission pipeline.
#[derive(Debug, Clone, Copy)]
pub struct MpcCommitController {
    /// Desired queue depth (setpoint).
    target: f64,
    /// State dynamics coefficient. `x_{k+1} = a * x_k + b * u_k`.
    /// Values in `[0, 1)` model exponential decay when `u = 0`.
    a: f64,
    /// Actuator gain — admission-rate to queue-depth sensitivity.
    b: f64,
    /// Previous admission rate, used for smoothing.
    u_prev: f64,
    /// Admission-rate cap. The controller never returns more than this.
    max_rate: f64,
    /// Control-effort weight. Higher `rho` means smaller control steps.
    rho: f64,
}

impl MpcCommitController {
    /// Build a controller with sensible defaults (`a = 0.9`, `b = 1.0`,
    /// `rho = 1.0`, `u_prev = 0.0`).
    #[must_use]
    pub fn new(target: f64, max_rate: f64) -> Self {
        Self::with_params(target, max_rate, 0.9, 1.0, DEFAULT_RHO)
    }

    /// Build a controller with explicit dynamics and smoothing parameters.
    #[must_use]
    pub fn with_params(target: f64, max_rate: f64, a: f64, b: f64, rho: f64) -> Self {
        let target = if target.is_finite() && target >= 0.0 {
            target
        } else {
            0.0
        };
        let max_rate = if max_rate.is_finite() && max_rate >= 0.0 {
            max_rate
        } else {
            0.0
        };
        let a = if a.is_finite() { a } else { 0.9 };
        let b = if b.is_finite() && b.abs() > MIN_B {
            b
        } else {
            1.0
        };
        let rho = if rho.is_finite() && rho >= 0.0 {
            rho
        } else {
            DEFAULT_RHO
        };
        Self {
            target,
            a,
            b,
            u_prev: 0.0,
            max_rate,
            rho,
        }
    }

    /// Current setpoint.
    #[must_use]
    pub fn target(&self) -> f64 {
        self.target
    }

    /// Last admission rate this controller emitted.
    #[must_use]
    pub fn last_rate(&self) -> f64 {
        self.u_prev
    }

    /// Advance the controller by one observation and return the next admission
    /// rate. Callers typically interpret this as "commits allowed per tick".
    pub fn step(&mut self, measured_queue_depth: f64) -> f64 {
        let x = if measured_queue_depth.is_finite() {
            measured_queue_depth.max(0.0)
        } else {
            0.0
        };

        // Unconstrained one-step LQR optimum: pick u so that the predicted
        // next state lands exactly on target, i.e.
        //      a * x + b * u_star = target
        //   => u_star = (target - a * x) / b
        //
        // When the queue is *below* target, `u_star` is positive and we admit
        // more to drive the queue up toward the setpoint. When the queue is
        // above target the optimum becomes negative and the `[0, max_rate]`
        // clamp pins it to zero, letting the natural drain coefficient `a` do
        // the work.
        let u_star_raw = self.a.mul_add(-x, self.target) / self.b;
        let u_star = u_star_raw.clamp(0.0, self.max_rate);

        // Smooth toward u_star. The factor 1 / (1 + rho) is the closed-form
        // minimiser of the smoothed cost once the unconstrained optimum has
        // been picked; `rho = 0` means "jump immediately", larger `rho` means
        // slower response.
        let alpha = 1.0 / (1.0 + self.rho);
        let u = alpha.mul_add(u_star - self.u_prev, self.u_prev);
        let u = u.clamp(0.0, self.max_rate);
        self.u_prev = u;
        u
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Simulate the plant `x_{k+1} = a*x_k + b*u_k + w_k` using the controller's
    /// own dynamics parameters so the test doesn't have to guess.
    fn simulate(
        ctrl: &mut MpcCommitController,
        x0: f64,
        steps: usize,
        disturbance: impl Fn(usize) -> f64,
    ) -> Vec<f64> {
        let a = ctrl.a;
        let b = ctrl.b;
        let mut x = x0;
        let mut trace = Vec::with_capacity(steps + 1);
        trace.push(x);
        for k in 0..steps {
            let u = ctrl.step(x);
            // Plant update with exogenous disturbance.
            x = (b.mul_add(u, a * x) + disturbance(k)).max(0.0);
            trace.push(x);
        }
        trace
    }

    #[test]
    fn mpc_commit_converges_to_target() {
        // Start at x=10, target=5, drain a=0.9, gain b=1.0, max_rate=10.
        let mut ctrl = MpcCommitController::with_params(5.0, 10.0, 0.9, 1.0, 1.0);
        let trace = simulate(&mut ctrl, 10.0, 20, |_| 0.0);
        let final_x = *trace.last().expect("non-empty trace");
        // After 20 steps we should be comfortably within 10% of target.
        assert!(
            (final_x - 5.0).abs() < 0.5,
            "expected convergence toward 5.0, got {final_x}; trace={trace:?}"
        );
        // And we should be strictly closer than the starting error.
        assert!((final_x - 5.0).abs() < (10.0_f64 - 5.0).abs());
    }

    #[test]
    fn mpc_commit_recovers_from_disturbance() {
        let mut ctrl = MpcCommitController::with_params(5.0, 10.0, 0.9, 1.0, 1.0);
        // Settle at target.
        let _ = simulate(&mut ctrl, 5.0, 40, |_| 0.0);
        // Inject a one-shot disturbance of +5 at k=0, then let it decay.
        let trace = simulate(&mut ctrl, 5.0, 30, |k| if k == 0 { 5.0 } else { 0.0 });
        let final_x = *trace.last().expect("non-empty trace");
        // Within 20% of target after recovery window.
        assert!(
            (final_x - 5.0).abs() < 1.0,
            "expected recovery within 20% of target, got {final_x}; trace={trace:?}"
        );
    }

    #[test]
    fn mpc_commit_rate_is_clamped() {
        let mut ctrl = MpcCommitController::with_params(0.0, 3.0, 0.9, 1.0, 0.0);
        // Huge queue with rho=0 would want a massive u; clamp must kick in.
        let u = ctrl.step(1_000.0);
        assert!((0.0..=3.0).contains(&u), "expected clamp to [0,3], got {u}");
    }

    #[test]
    fn mpc_commit_sanitises_bad_inputs() {
        // NaN / infinite / negative inputs all get replaced with safe defaults.
        let ctrl = MpcCommitController::with_params(
            f64::NAN,
            f64::INFINITY,
            f64::NAN,
            0.0, // below MIN_B -> defaults to 1.0
            -1.0,
        );
        assert_eq!(ctrl.target(), 0.0);
        assert_eq!(ctrl.last_rate(), 0.0);
    }
}

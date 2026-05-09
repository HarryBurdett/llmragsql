/**
 * Balance Check plugin — UMD bundle entry.
 */
import BalanceCheck from './BalanceCheck';

if (typeof window !== 'undefined') {
  window.__SAM_APPS__ = window.__SAM_APPS__ ?? {};
  window.__SAM_APPS__['balance-check'] = {
    id: 'balance-check',
    component: BalanceCheck as unknown as (props: {
      context: import('./sam').SamPluginContext;
    }) => unknown,
  };
}

export default BalanceCheck;

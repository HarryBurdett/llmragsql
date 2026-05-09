/**
 * Suppliers plugin — UMD bundle entry.
 */
import './index.css';
import Suppliers from './Suppliers';

if (typeof window !== 'undefined') {
  window.__SAM_APPS__ = window.__SAM_APPS__ ?? {};
  window.__SAM_APPS__['suppliers'] = {
    id: 'suppliers',
    component: Suppliers as unknown as (props: {
      context: import('./sam').SamPluginContext;
    }) => unknown,
  };
}

export default Suppliers;

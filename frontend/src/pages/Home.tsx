import { LayoutDashboard } from 'lucide-react';
import { EmptyState } from '../components/ui';

export function Home() {
  return (
    <EmptyState
      icon={LayoutDashboard}
      title="Welcome"
      message="Select an option from the menu above"
    />
  );
}

export default Home;

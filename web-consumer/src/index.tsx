import { render } from 'solid-js/web';
import App from './App';
import './style.css';

const root = document.getElementById('app');
if (root) {
  render(() => <App />, root);
}

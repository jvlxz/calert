## Alerting

**Alert Instance**: A single firing or resolved alert occurrence emitted by Alertmanager. _Avoid_: alert when referring specifically to one fingerprinted occurrence.

**Alert Notification**: A batch delivered by Alertmanager that contains one or more **Alert Instances**. _Avoid_: message, card.

**Chat Card**: The visual Google Chat message body that may summarize one or more **Alert Instances** from an **Alert Notification**. _Avoid_: thread.

**Chat Thread**: The Google Chat conversation context that contains related **Chat Cards** over time. _Avoid_: card.

An **Alert Notification** contains one or more **Alert Instances**.
An **Alert Notification** can produce one or more **Chat Cards**.
A **Chat Thread** can contain multiple **Chat Cards**.

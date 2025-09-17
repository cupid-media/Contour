using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Contour.Tracing
{
    using System.Diagnostics;

    /// <summary>
    /// Extension methods for convenient distributed tracing with messages
    /// </summary>
    public static class MessageTracingExtensions
    {
        /// <summary>
        /// Starts a new activity with trace context from the message as parent
        /// </summary>
        /// <param name="message">The message to extract trace context from</param>
        /// <param name="activitySource">Activity source to create the activity</param>
        /// <param name="name">Activity name. Defaults to caller method name.</param>
        /// <param name="kind">Activity kind (default: Consumer)</param>
        /// <returns>New activity with proper parent context, or null if activity source is not enabled</returns>
        public static Activity? StartActivityFromMessage<T>(
            this Message<T> message, 
            ActivitySource activitySource, 
            [CallerMemberName] string name = "", 
            ActivityKind kind = ActivityKind.Consumer) where T : class
        {
            if (!message.TryGetParentActivityContext(out var parentContext))
            {
                // Create activity without parent context if extraction fails
                return activitySource.StartActivity(name, kind);
            }

            return activitySource.StartActivity(name, kind, parentContext);
        }

        /// <summary>
        /// Starts a new activity with trace context from the message as parent, with tags
        /// </summary>
        /// <param name="message">The message to extract trace context from</param>
        /// <param name="activitySource">Activity source to create the activity</param>
        /// <param name="name">Activity name</param>
        /// <param name="kind">Activity kind</param>
        /// <param name="tags">Activity tags</param>
        /// <returns>New activity with proper parent context and tags</returns>
        public static Activity? StartActivityFromMessage<T>(
            this Message<T> message, 
            ActivitySource activitySource, 
            string name, 
            ActivityKind kind, 
            IEnumerable<KeyValuePair<string, object>> tags) where T : class
        {
            Activity activity;
            
            if (!message.TryGetParentActivityContext(out var parentContext))
            {
                activity = activitySource.StartActivity(name, kind, default(ActivityContext), tags);
            }
            else
            {
                activity = activitySource.StartActivity(name, kind, parentContext, tags);
            }

            return activity;
        }
    }
}

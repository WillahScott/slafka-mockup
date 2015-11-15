using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(BeyondAdmin.WebForms.Startup))]
namespace BeyondAdmin.WebForms
{
    public partial class Startup {
        public void Configuration(IAppBuilder app) {
            ConfigureAuth(app);
        }
    }
}

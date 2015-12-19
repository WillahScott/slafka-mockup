using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace BeyondAdmin.WebForms
{
    public partial class Arabic : System.Web.UI.MasterPage
    {
        protected void Page_Load(object sender, EventArgs e)
        {

        }
        protected void SideBarMenu_MenuItemDataBound(object sender, MenuEventArgs e)
        {
            var node = e.Item.DataItem as SiteMapNode;

            if (node != null && !string.IsNullOrEmpty(node["imageUrl"]))
                e.Item.ImageUrl = node["imageUrl"];

            if (!string.IsNullOrEmpty(e.Item.NavigateUrl) && Request.Url.AbsolutePath.ToLower().Contains(Page.ResolveUrl(e.Item.NavigateUrl.ToLower().Replace(".aspx", ""))))
            {
                e.Item.Selected = true;
            }

        }
    }
}
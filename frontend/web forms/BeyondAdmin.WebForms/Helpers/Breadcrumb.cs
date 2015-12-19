using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace BeyondAdmin.WebForms.Helpers
{
    [DefaultProperty("Text")]
    [ToolboxData("<{0}:Breadcrumb runat=\"server\"></{0}:Breadcrumb>")]
    public class Breadcrumb : SiteMapPath
    {
        public Breadcrumb()
        {
            this.PathSeparator = null;
            this.RenderCurrentNodeAsLink = false;
            ShowToolTips = false;
        }

        protected override void Render(HtmlTextWriter writer)
        {
            var node = this.Provider.CurrentNode;
            var nodes = new Stack<SiteMapNode>();

            for (var currentNode = node; currentNode != null; currentNode = currentNode.ParentNode)
            {
                nodes.Push(currentNode);
            }

            var sb = new System.Text.StringBuilder();

            sb.AppendLine(@"<ul class=""breadcrumb"" " + this.CssClass + ">");

            foreach (var currentNode in nodes)
            {
                var siteMapNode = this.Provider.CurrentNode;
                if (siteMapNode != null && currentNode.Url == siteMapNode.Url)
                {
                    sb.AppendLine(@"<li class=""active"">" + currentNode.Title + "</li>");
                }
                else
                {
                    if (currentNode != null)
                        if (currentNode.ParentNode == null)
                            sb.AppendLine(@"<li><i class='fa fa-home'></i><a href=""" + currentNode.Url + @""">" + currentNode.Title + "</a></li>");
                        else
                            sb.AppendLine(@"<li><a href=""" + currentNode.Url + @""">" + currentNode.Title + "</a></li>");
                }
            }

            sb.AppendLine(@"</ul>");

            if (writer != null) writer.Write(sb.ToString());
        }
    }

}